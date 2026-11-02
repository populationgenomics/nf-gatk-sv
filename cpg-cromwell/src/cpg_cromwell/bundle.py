"""
WDL bundles (design §7.1): replaces `prepare_git_job` + `zip -r tools.zip`.

A bundle lives at `<dest>/<repo>/<commit>/` and holds `deps.zip`, every `.wdl` file under
`cwd` (relative paths kept), and `bundle.json`, which is written last.

Security: for non-`test` access levels the commit must be an ancestor of origin/main. That is
checked here and recorded as `verified_on_main`; submission re-checks the record. This relies on
`<dest>` being writable only by the access level (e.g. the dataset's tmp bucket).
"""

import datetime
import json
import logging
import os
import re
import subprocess
import tempfile
import zipfile
from collections.abc import Sequence
from concurrent.futures import ThreadPoolExecutor
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Any, Self

from cpg_cromwell.errors import BundleError, SpecError
from cpg_cromwell.spec import WorkflowSource
from cpg_cromwell.storage import Storage, join

logger = logging.getLogger(__name__)

MANIFEST = 'bundle.json'
DEPS_ZIP = 'deps.zip'
_SHA_RE = re.compile(r'^[0-9a-f]{40}$')
_NAME_RE = re.compile(r'^[A-Za-z0-9._-]+$')
_ZIP_EPOCH = (1980, 1, 1, 0, 0, 0)


@dataclass(frozen=True)
class BundleManifest:
    repo: str
    org: str
    commit: str
    cwd: str
    libs: list[str]
    verified_on_main: bool
    created_at: str

    def to_json(self) -> str:
        return json.dumps(asdict(self), indent=2)

    @classmethod
    def from_json(cls, text: str, where: str) -> Self:
        try:
            data = json.loads(text)
            return cls(**{f: data[f] for f in cls.__dataclass_fields__})
        except (ValueError, KeyError, TypeError) as e:
            raise BundleError(f'{where}: malformed {MANIFEST}: {e}') from e


@dataclass(frozen=True)
class LoadedBundle:
    uri: str
    manifest: BundleManifest
    workflow: str
    workflow_source: bytes
    dependencies: bytes | None


def bundle_uri(dest: str, repo: str, commit: str) -> str:
    return join(dest, repo, commit) + '/'


def _git(*args: str, cwd: Path | None = None, config: Sequence[str] = ()) -> subprocess.CompletedProcess[str]:
    cmd = ['git']
    for c in config:
        cmd += ['-c', c]
    cmd += list(args)
    return subprocess.run(cmd, cwd=cwd, capture_output=True, text=True, check=False)


def _git_ok(*args: str, cwd: Path | None = None, config: Sequence[str] = ()) -> str:
    r = _git(*args, cwd=cwd, config=config)
    if r.returncode != 0:
        raise BundleError(f'git {" ".join(args)} failed ({r.returncode}): {r.stderr.strip()[-2000:]}')
    return r.stdout


def _git_credentials_config(tmp: Path) -> list[str]:
    """
    Same source as prepare_git_job: [infrastructure].git_credentials_secret_{name,project}.
    Written to a private temp file instead of ~/.git-credentials.
    """
    try:
        from cpg_utils.config import config_retrieve  # noqa: PLC0415

        name = config_retrieve(['infrastructure', 'git_credentials_secret_name'], default='')
        project = config_retrieve(['infrastructure', 'git_credentials_secret_project'], default='')
    except Exception:  # noqa: BLE001  (no config: public repos only, as before)
        return []
    if not (name and project):
        logger.info('no git credentials secret configured; private repositories cannot be cloned')
        return []
    from cpg_utils.cloud import read_secret  # noqa: PLC0415

    value = read_secret(project, name)
    if not value:
        logger.warning('could not read git credentials secret %s in %s', name, project)
        return []
    path = tmp / 'git-credentials'
    path.touch(mode=0o600)
    path.write_text(value)
    return [f'credential.helper=store --file={path}']


def _iter_files(root: Path) -> list[Path]:
    files = []
    for dirpath, dirnames, filenames in os.walk(root, followlinks=True):
        dirnames[:] = sorted(d for d in dirnames if d != '.git')
        files.extend(Path(dirpath) / f for f in sorted(filenames))
    return files


def make_deps_zip(cwd: Path, libs: Sequence[str], dest: Path) -> None:
    """
    Reproducible equivalent of `cd <cwd> && zip -r deps.zip <lib>/...`: sorted entries,
    fixed timestamps, no directory entries, `.git` skipped. Arcnames are relative to cwd.
    """
    base = cwd.resolve()
    entries: dict[str, Path] = {}
    for lib in libs:
        root = (base / lib).resolve()
        if not root.is_relative_to(base):
            raise SpecError(f'lib {lib!r} is outside cwd')
        if root.is_file():
            entries[os.path.relpath(root, base)] = root
            continue
        if not root.is_dir():
            raise BundleError(f'lib {lib!r} does not exist under {cwd.name or "the repo"}')
        for f in _iter_files(root):
            entries[os.path.relpath(f, base)] = f
    with zipfile.ZipFile(dest, 'w', compression=zipfile.ZIP_DEFLATED) as zf:
        for arcname in sorted(entries):
            src = entries[arcname]
            info = zipfile.ZipInfo(arcname.replace(os.sep, '/'), date_time=_ZIP_EPOCH)
            mode = 0o755 if os.access(src, os.X_OK) else 0o644
            info.external_attr = (0o100000 | mode) << 16
            info.compress_type = zipfile.ZIP_DEFLATED
            zf.writestr(info, src.read_bytes())


def _validate_args(repo: str, commit: str, cwd: str, libs: Sequence[str], organisation: str) -> None:
    if not _NAME_RE.match(repo) or not _NAME_RE.match(organisation):
        raise SpecError(f'invalid repo or organisation: {organisation}/{repo}')
    if not _SHA_RE.match(commit):
        # a branch or short SHA is mutable/ambiguous; the bundle path and re-attach key need the identity
        raise SpecError(f'commit must be a full 40-character lowercase SHA, got {commit!r}')
    for p in (cwd, *libs):
        if p.startswith('/') or '..' in Path(p).parts:
            raise SpecError(f'cwd and libs must be relative paths inside the repo: {p!r}')


def build_bundle(
    repo: str,
    commit: str,
    *,
    cwd: str = '',
    libs: Sequence[str] = ('.',),
    organisation: str = 'populationgenomics',
    dest: str,
    require_main: bool,
    storage: Storage | None = None,
    repo_url: str | None = None,
) -> str:
    """
    Build (or reuse) the bundle for repo@commit and return its URI.
    `repo_url` overrides https://github.com/<organisation>/<repo>.git (tests, mirrors).
    """
    _validate_args(repo, commit, cwd, libs, organisation)
    storage = storage or Storage()
    uri = bundle_uri(dest, repo, commit)
    manifest_uri = uri + MANIFEST

    existing = storage.read_text(manifest_uri)
    if existing is not None:
        manifest = BundleManifest.from_json(existing, manifest_uri)
        if (manifest.cwd, list(manifest.libs), manifest.org) != (cwd, list(libs), organisation):
            raise BundleError(
                f'{uri} already exists with cwd={manifest.cwd!r} libs={manifest.libs} org={manifest.org}; '
                f'asked for cwd={cwd!r} libs={list(libs)} org={organisation}',
            )
        if manifest.verified_on_main or not require_main:
            logger.info('bundle already exists: %s (verified_on_main=%s)', uri, manifest.verified_on_main)
            return uri
        logger.info('bundle exists but is not verified on main; verifying and rebuilding it')

    url = repo_url or f'https://github.com/{organisation}/{repo}.git'
    with tempfile.TemporaryDirectory(prefix='cpg-cromwell-bundle-') as tmp_str:
        tmp = Path(tmp_str)
        git_config = _git_credentials_config(tmp)
        checkout = tmp / 'repo'
        logger.info('cloning %s', url)
        _git_ok('clone', '--quiet', '--recurse-submodules', url, str(checkout), config=git_config)

        r = _git('merge-base', '--is-ancestor', commit, 'origin/main', cwd=checkout)
        if r.returncode not in (0, 1):
            raise BundleError(f'could not check {commit} against origin/main: {r.stderr.strip()}')
        verified = r.returncode == 0
        if require_main and not verified:
            raise BundleError(f'error: commit {commit} is not merged into the main branch of {organisation}/{repo}')

        _git_ok('checkout', '--quiet', commit, cwd=checkout)
        _git_ok('submodule', 'update', '--init', '--recursive', '--quiet', cwd=checkout, config=git_config)

        work = (checkout / cwd).resolve()
        if not work.is_dir() or not work.is_relative_to(checkout.resolve()):
            raise BundleError(f'cwd {cwd!r} is not a directory in {repo}@{commit}')

        uploads: list[tuple[Path, str]] = []
        if libs:
            deps = tmp / DEPS_ZIP
            make_deps_zip(work, libs, deps)
            uploads.append((deps, uri + DEPS_ZIP))
        for f in _iter_files(work):
            if f.suffix == '.wdl':
                uploads.append((f, uri + os.path.relpath(f, work).replace(os.sep, '/')))
        logger.info('uploading %d files to %s', len(uploads), uri)
        with ThreadPoolExecutor(max_workers=16) as pool:
            list(pool.map(lambda u: storage.upload(*u), uploads))

        manifest = BundleManifest(
            repo=repo,
            org=organisation,
            commit=commit,
            cwd=cwd,
            libs=list(libs),
            verified_on_main=verified,
            created_at=datetime.datetime.now(datetime.UTC).isoformat(timespec='seconds'),
        )
        storage.write(manifest_uri, manifest.to_json())  # last: its presence means the bundle is complete
    logger.info('bundle ready: %s (verified_on_main=%s)', uri, verified)
    return uri


def read_manifest(bundle: str, storage: Storage) -> BundleManifest:
    uri = bundle.rstrip('/') + '/' + MANIFEST
    text = storage.read_text(uri)
    if text is None:
        raise BundleError(f'no {MANIFEST} at {uri}: build the bundle with `cpg-cromwell bundle` first')
    return BundleManifest.from_json(text, uri)


def check_submittable(manifest: BundleManifest, access_level: str, where: str) -> None:
    """Submit-time check (§7.1.6): non-test access levels need a bundle verified on main."""
    if access_level != 'test' and manifest.verified_on_main is not True:
        raise BundleError(
            f'{where} is not verified on main (verified_on_main={manifest.verified_on_main!r}); '
            f'access level {access_level!r} may only run WDL from commits merged into main',
        )


def load_bundle(source: WorkflowSource, access_level: str, storage: Storage) -> LoadedBundle:
    manifest = read_manifest(source.bundle, storage)
    check_submittable(manifest, access_level, source.bundle_root)
    wdl = storage.read(source.workflow_uri)
    if wdl is None:
        raise BundleError(f'workflow {source.workflow!r} not found in bundle {source.bundle_root}')
    deps = None
    if manifest.libs:
        blob = storage.read(source.bundle_root + DEPS_ZIP)
        if blob is None:
            raise BundleError(f'{DEPS_ZIP} missing from bundle {source.bundle_root}')
        deps = blob.data
    return LoadedBundle(source.bundle_root, manifest, source.workflow, wdl.data, deps)


def manifest_dict(manifest: BundleManifest) -> dict[str, Any]:
    return asdict(manifest)
