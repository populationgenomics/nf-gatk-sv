"""Thin REST wrapper around the Cromwell API (design §4). One request per call; callers retry."""

import json
import logging
import threading
from collections.abc import Iterable
from typing import Any

import requests

from cpg_cromwell.auth import TokenProvider
from cpg_cromwell.errors import TransientError

logger = logging.getLogger(__name__)

TERMINAL_STATUSES = frozenset({'Succeeded', 'Failed', 'Aborted'})
ACTIVE_STATUSES = frozenset({'Submitted', 'Running', 'On Hold'})
ABORTING_STATUS = 'Aborting'

# trimmed metadata for display()/failures, so a GatherBatchEvidence-sized document doesn't blow up memory
DISPLAY_METADATA_KEYS = (
    'id',
    'workflowName',
    'status',
    'submission',
    'start',
    'end',
    'failures',
    'calls',
    'executionStatus',
    'shardIndex',
    'attempt',
    'backendStatus',
)


class CromwellUnavailable(TransientError):
    """Connection error, timeout, 5xx, 429 or 401: worth retrying."""


class CromwellRequestError(Exception):
    """A 4xx response (other than 401/429)."""

    def __init__(self, status_code: int, message: str) -> None:
        super().__init__(f'Cromwell returned {status_code}: {message}')
        self.status_code = status_code


class CromwellNotFound(CromwellRequestError):
    pass


class CromwellApi:
    """
    `url` and `audience` default to cpg_utils.constants (overridable with the
    CROMWELL_URL / CROMWELL_AUDIENCE env vars).
    """

    def __init__(
        self,
        url: str | None = None,
        audience: str | None = None,
        *,
        token_provider: TokenProvider | None = None,
        timeout: float = 60,
        submit_timeout: float = 300,
    ) -> None:
        if url is None or audience is None:
            from cpg_utils.constants import CROMWELL_AUDIENCE, CROMWELL_URL  # noqa: PLC0415

            url = url or CROMWELL_URL
            audience = audience or CROMWELL_AUDIENCE
        self.url = url.rstrip('/')
        self.tokens = token_provider or TokenProvider(audience)
        self.timeout = timeout
        self.submit_timeout = submit_timeout
        self._local = threading.local()

    @property
    def _session(self) -> requests.Session:
        session = getattr(self._local, 'session', None)
        if session is None:
            session = self._local.session = requests.Session()
        return session

    def _request(self, method: str, path: str, *, timeout: float | None = None, **kwargs: Any) -> requests.Response:
        headers = {'Authorization': f'Bearer {self.tokens.token()}', 'Accept': 'application/json'}
        url = f'{self.url}/api/workflows/v1{path}'
        try:
            r = self._session.request(method, url, headers=headers, timeout=timeout or self.timeout, **kwargs)
        except requests.RequestException as e:
            raise CromwellUnavailable(f'{method} {url}: {e}') from e
        if r.ok:
            return r
        text = r.text[:500]
        if r.status_code == 401:
            self.tokens.invalidate()
        if r.status_code >= 500 or r.status_code in (401, 408, 429):
            raise CromwellUnavailable(f'{method} {url}: HTTP {r.status_code}: {text}')
        if r.status_code == 404:
            raise CromwellNotFound(r.status_code, text)
        raise CromwellRequestError(r.status_code, text)

    @staticmethod
    def _json(r: requests.Response) -> Any:
        try:
            return r.json()
        except ValueError as e:
            raise CromwellUnavailable(f'Cromwell returned a non-JSON body: {r.text[:200]}') from e

    def submit(
        self,
        workflow_source: bytes,
        inputs: dict[str, Any],
        options: dict[str, Any],
        dependencies: bytes | None = None,
        *,
        workflow_filename: str = 'workflow.wdl',
    ) -> str:
        files: dict[str, tuple[str, bytes, str]] = {
            'workflowSource': (workflow_filename, workflow_source, 'application/octet-stream'),
            # json.dumps with default separators: the same bytes the old `echo '{json.dumps(...)}'` produced
            'workflowInputs': ('inputs.json', json.dumps(inputs).encode(), 'application/json'),
            'workflowOptions': ('workflow-options.json', json.dumps(options).encode(), 'application/json'),
        }
        if dependencies is not None:
            files['workflowDependencies'] = ('deps.zip', dependencies, 'application/zip')
        r = self._request('POST', '', files=files, timeout=self.submit_timeout)
        body = self._json(r)
        workflow_id = body.get('id') if isinstance(body, dict) else None
        if not workflow_id:
            raise CromwellUnavailable(f'submission response had no workflow id: {body!r}')
        return workflow_id

    def status(self, workflow_id: str) -> str:
        body = self._json(self._request('GET', f'/{workflow_id}/status'))
        status = body.get('status') if isinstance(body, dict) else None
        if not status:
            raise CromwellUnavailable(f'status response had no status: {body!r}')
        return status

    def metadata(
        self,
        workflow_id: str,
        include_keys: Iterable[str] | None = None,
        *,
        expand_subworkflows: bool = False,
    ) -> dict[str, Any]:
        params: list[tuple[str, str]] = [('includeKey', k) for k in include_keys or ()]
        if expand_subworkflows:
            params.append(('expandSubWorkflows', 'true'))
        return self._json(self._request('GET', f'/{workflow_id}/metadata', params=params))

    def outputs(self, workflow_id: str) -> dict[str, Any]:
        body = self._json(self._request('GET', f'/{workflow_id}/outputs'))
        outputs = body.get('outputs') if isinstance(body, dict) else None
        if not isinstance(outputs, dict):
            raise CromwellUnavailable(f'outputs response had no outputs: {body!r}')
        return outputs

    def abort(self, workflow_id: str) -> str:
        body = self._json(self._request('POST', f'/{workflow_id}/abort'))
        return body.get('status', 'Aborting') if isinstance(body, dict) else 'Aborting'

    def query(self, workflow_ids: Iterable[str]) -> dict[str, str]:
        """Batch status lookup (optional optimisation, design §7.5)."""
        params = [('id', w) for w in workflow_ids]
        if not params:
            return {}
        params += [('includeSubworkflows', 'false')]
        body = self._json(self._request('GET', '/query', params=params))
        return {r['id']: r['status'] for r in body.get('results', []) if 'id' in r and 'status' in r}
