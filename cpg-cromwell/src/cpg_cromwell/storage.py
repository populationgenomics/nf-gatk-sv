"""
Small object-storage layer over GCS and the local filesystem.

Only what the package needs: read with generation, conditional write, list, and copy
(server-side `rewrite` for gs:// -> gs://). GCS errors are translated into the exit-code
contract: 5xx/429 after retries -> TransientError; NotFound/Forbidden -> CollectError.
"""

import logging
import os
import shutil
import tempfile
import threading
from collections.abc import Iterator
from dataclasses import dataclass
from pathlib import Path

from cpg_cromwell.errors import Cancelled, CollectError, TransientError

logger = logging.getLogger(__name__)


class PreconditionFailed(Exception):
    """A generation precondition did not hold (someone else wrote first)."""


@dataclass(frozen=True)
class Blob:
    data: bytes
    generation: int


def is_gcs(uri: str) -> bool:
    return uri.startswith('gs://')


def join(base: str, *parts: str) -> str:
    out = base
    for part in parts:
        out = out.rstrip('/') + '/' + part.lstrip('/')
    return out


def split_gcs(uri: str) -> tuple[str, str]:
    if not is_gcs(uri):
        raise ValueError(f'not a gs:// URI: {uri}')
    bucket, _, name = uri[len('gs://') :].partition('/')
    if not bucket:
        raise ValueError(f'no bucket in {uri}')
    return bucket, name


class LocalBackend:
    """Local files. Generation is emulated with st_mtime_ns; good enough for one host."""

    _lock = threading.Lock()

    def path(self, uri: str) -> Path:
        return Path(uri)

    def read(self, uri: str) -> Blob | None:
        p = self.path(uri)
        try:
            with self._lock:
                return Blob(p.read_bytes(), p.stat().st_mtime_ns)
        except FileNotFoundError:
            return None

    def write(self, uri: str, data: bytes, *, if_generation_match: int | None = None) -> None:
        p = self.path(uri)
        p.parent.mkdir(parents=True, exist_ok=True)
        with self._lock:
            if if_generation_match is not None:
                current = p.stat().st_mtime_ns if p.exists() else 0
                if current != if_generation_match:
                    raise PreconditionFailed(uri)
            fd, tmp = tempfile.mkstemp(dir=p.parent, prefix=f'.{p.name}.')
            with os.fdopen(fd, 'wb') as fh:
                fh.write(data)
            os.replace(tmp, p)

    def exists(self, uri: str) -> bool:
        return self.path(uri).exists()

    def list(self, prefix: str) -> Iterator[str]:
        root = self.path(prefix)
        if root.is_dir():
            for p in sorted(root.rglob('*')):
                if p.is_file():
                    yield str(p)

    def download(self, uri: str, dest: Path) -> None:
        shutil.copyfile(self.path(uri), dest)

    def upload(self, src: Path, uri: str) -> None:
        p = self.path(uri)
        p.parent.mkdir(parents=True, exist_ok=True)
        shutil.copyfile(src, p)


def _translate_gcs_error(e: Exception, what: str) -> Exception:
    from google.api_core import exceptions as gexc  # noqa: PLC0415

    if isinstance(e, gexc.PreconditionFailed):
        return PreconditionFailed(what)
    if isinstance(e, gexc.NotFound):
        return CollectError(f'{what}: not found')
    if isinstance(e, gexc.Forbidden | gexc.Unauthorized):
        return CollectError(f'{what}: permission denied: {e}')
    if isinstance(e, gexc.TooManyRequests | gexc.ServerError | gexc.RetryError | ConnectionError | TimeoutError):
        return TransientError(f'{what}: {e}')
    if isinstance(e, gexc.GoogleAPICallError):
        return CollectError(f'{what}: {e}')
    try:
        import requests  # noqa: PLC0415

        if isinstance(e, requests.RequestException):
            return TransientError(f'{what}: {e}')
    except ImportError:  # pragma: no cover
        pass
    return e


class GcsBackend:
    def __init__(self, client: object | None = None) -> None:
        self._client = client
        self._client_lock = threading.Lock()

    @property
    def client(self):
        with self._client_lock:
            if self._client is None:
                from google.cloud import storage  # noqa: PLC0415

                self._client = storage.Client()
            return self._client

    def _blob(self, uri: str):
        bucket, name = split_gcs(uri)
        return self.client.bucket(bucket).blob(name)

    def read(self, uri: str) -> Blob | None:
        from google.api_core import exceptions as gexc  # noqa: PLC0415

        blob = self._blob(uri)
        try:
            data = blob.download_as_bytes()
        except gexc.NotFound:
            return None
        except Exception as e:
            raise _translate_gcs_error(e, f'reading {uri}') from e
        if blob.generation is None:
            blob.reload()
        return Blob(data, int(blob.generation))

    def write(self, uri: str, data: bytes, *, if_generation_match: int | None = None) -> None:
        from google.cloud.storage.retry import DEFAULT_RETRY  # noqa: PLC0415

        blob = self._blob(uri)
        try:
            # with a generation precondition the upload is idempotent, so the library retries it;
            # without one we retry too: every write here is a whole-object replace of our own data
            blob.upload_from_string(data, if_generation_match=if_generation_match, retry=DEFAULT_RETRY)
        except Exception as e:
            raise _translate_gcs_error(e, f'writing {uri}') from e

    def exists(self, uri: str) -> bool:
        try:
            return self._blob(uri).exists()
        except Exception as e:
            raise _translate_gcs_error(e, f'checking {uri}') from e

    def list(self, prefix: str) -> Iterator[str]:
        bucket, name = split_gcs(prefix)
        try:
            for blob in self.client.list_blobs(bucket, prefix=name):
                yield f'gs://{bucket}/{blob.name}'
        except Exception as e:
            raise _translate_gcs_error(e, f'listing {prefix}') from e

    def download(self, uri: str, dest: Path) -> None:
        try:
            self._blob(uri).download_to_filename(str(dest))
        except Exception as e:
            raise _translate_gcs_error(e, f'downloading {uri}') from e

    def upload(self, src: Path, uri: str) -> None:
        from google.cloud.storage.retry import DEFAULT_RETRY  # noqa: PLC0415

        try:
            self._blob(uri).upload_from_filename(str(src), retry=DEFAULT_RETRY)
        except Exception as e:
            raise _translate_gcs_error(e, f'uploading {uri}') from e

    def same_object(self, src: str, dst: str) -> bool:
        """True if dst exists with the same crc32c as src (skip the copy on re-collection)."""
        from google.api_core import exceptions as gexc  # noqa: PLC0415

        s, d = self._blob(src), self._blob(dst)
        try:
            s.reload()
            d.reload()
        except gexc.NotFound:
            return False
        return bool(s.crc32c) and s.crc32c == d.crc32c and s.size == d.size

    def rewrite(self, src: str, dst: str, cancel: threading.Event | None = None) -> None:
        """Server-side copy; loops on the rewrite token so multi-GB objects work."""
        from google.cloud.storage.retry import DEFAULT_RETRY  # noqa: PLC0415

        s, d = self._blob(src), self._blob(dst)
        token = None
        try:
            while True:
                token, done, total = d.rewrite(s, token=token, retry=DEFAULT_RETRY)
                if token is None:
                    return
                logger.debug('rewrite %s -> %s: %s/%s bytes', src, dst, done, total)
                if cancel is not None and cancel.is_set():
                    raise Cancelled(f'copy {src} -> {dst} cancelled')
        except Cancelled:
            raise
        except Exception as e:
            raise _translate_gcs_error(e, f'copying {src} -> {dst}') from e


class Storage:
    """Routes gs:// URIs to GCS and everything else to the local filesystem."""

    def __init__(self, gcs: GcsBackend | None = None, local: LocalBackend | None = None) -> None:
        self.gcs = gcs or GcsBackend()
        self.local = local or LocalBackend()

    def backend(self, uri: str) -> GcsBackend | LocalBackend:
        return self.gcs if is_gcs(uri) else self.local

    def read(self, uri: str) -> Blob | None:
        return self.backend(uri).read(uri)

    def read_text(self, uri: str) -> str | None:
        blob = self.read(uri)
        return None if blob is None else blob.data.decode()

    def write(self, uri: str, data: bytes | str, *, if_generation_match: int | None = None) -> None:
        if isinstance(data, str):
            data = data.encode()
        self.backend(uri).write(uri, data, if_generation_match=if_generation_match)

    def exists(self, uri: str) -> bool:
        return self.backend(uri).exists(uri)

    def list(self, prefix: str) -> Iterator[str]:
        return self.backend(prefix).list(prefix)

    def upload(self, src: Path, uri: str) -> None:
        self.backend(uri).upload(src, uri)

    def copy(self, src: str, dst: str, *, cancel: threading.Event | None = None) -> bool:
        """Copy one object. Returns False if it was skipped (same path, or identical object already there)."""
        if src == dst:
            return False
        if cancel is not None and cancel.is_set():
            raise Cancelled(f'copy {src} -> {dst} cancelled')
        sb, db = self.backend(src), self.backend(dst)
        if isinstance(sb, GcsBackend) and isinstance(db, GcsBackend):
            if sb.same_object(src, dst):
                logger.info('already copied: %s', dst)
                return False
            sb.rewrite(src, dst, cancel)
            return True
        with tempfile.TemporaryDirectory() as tmp:
            staged = Path(tmp) / 'object'
            try:
                sb.download(src, staged)
            except FileNotFoundError as e:
                raise CollectError(f'{src}: not found') from e
            db.upload(staged, dst)
        return True
