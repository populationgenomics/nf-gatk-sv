"""Shared test fixtures: a fake Cromwell server, directory-backed 'GCS', config and bundles."""

import io
import json
import threading
import uuid
import zipfile
from collections.abc import Iterator
from dataclasses import dataclass, field
from email.parser import BytesParser
from email.policy import default as email_policy
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from typing import Any, Self

import pytest

from cpg_cromwell.api import CromwellApi
from cpg_cromwell.auth import StaticTokenProvider
from cpg_cromwell.bundle import BundleManifest
from cpg_cromwell.run import RunContext
from cpg_cromwell.spec import PollPolicy, WorkflowSource, WorkflowSpec
from cpg_cromwell.storage import LocalBackend, Storage

FIXTURES = Path(__file__).parent / 'fixtures'
PARITY = json.loads((FIXTURES / 'cromwell_options_parity.json').read_text())
SECRET = PARITY['secret_json']
COMMIT = '0123456789abcdef0123456789abcdef01234567'
BUNDLE = f'gs://bucket/bundles/repo/{COMMIT}/'
FAST = PollPolicy(min_seconds=0.01, max_seconds=0.05, ramp_seconds=1, max_consecutive_errors=5)


# ---- storage --------------------------------------------------------------------------------


class DirBackend(LocalBackend):
    """gs://bucket/path -> <root>/bucket/path; local paths unchanged."""

    def __init__(self, root: Path) -> None:
        self.root = root

    def path(self, uri: str) -> Path:
        if uri.startswith('gs://'):
            return self.root / uri[len('gs://') :]
        return Path(uri)


class DirStorage(Storage):
    def __init__(self, root: Path) -> None:
        backend = DirBackend(root)
        super().__init__(gcs=None, local=backend)  # type: ignore[arg-type]
        self.gcs = backend  # type: ignore[assignment]

    def backend(self, uri: str) -> LocalBackend:  # type: ignore[override]
        return self.local

    def put(self, uri: str, data: str | bytes) -> None:
        self.write(uri, data)

    def text(self, uri: str) -> str | None:
        return self.read_text(uri)


@pytest.fixture
def storage(tmp_path: Path) -> DirStorage:
    return DirStorage(tmp_path / 'gcs')


# ---- config ---------------------------------------------------------------------------------


@pytest.fixture
def cpg_config(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> Iterator[Path]:
    from cpg_utils import config as cpg_config_module

    path = tmp_path / 'config.toml'
    path.write_text(PARITY['config_toml'])
    monkeypatch.setenv('CPG_CONFIG_PATH', str(path))
    cpg_config_module.set_config_paths([str(path)])
    yield path
    cpg_config_module._config_paths = []
    cpg_config_module._config = None


# ---- bundles --------------------------------------------------------------------------------


def write_bundle(storage: Storage, *, verified: bool = True, bundle: str = BUNDLE, workflow: str = 'Wfl.wdl') -> None:
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, 'w') as zf:
        zf.writestr('Structs.wdl', 'version 1.0\n')
    storage.write(bundle + 'deps.zip', buf.getvalue())
    storage.write(bundle + workflow, 'version 1.0\nworkflow Wfl {}\n')
    manifest = BundleManifest('repo', 'populationgenomics', COMMIT, 'wdl', ['.'], verified, '2026-11-02T00:00:00+00:00')
    storage.write(bundle + 'bundle.json', manifest.to_json())


def make_spec(**overrides: Any) -> WorkflowSpec:
    values: dict[str, Any] = {
        'name': 'Wfl/fixture-ds',
        'source': WorkflowSource(BUNDLE, 'Wfl.wdl'),
        'inputs': {'x': 1, 'vcf': 'gs://in/a.vcf'},
        'input_prefix': 'Wfl',
        'dataset': 'fixture-ds',
        'access_level': 'test',
        'output_prefix': 'gatk_sv/output/Wfl/fixture-ds',
        'ar_guid': 'guid-1',
        'labels': {'stage': 'wfl'},
        'collect': {'out': 'gs://dest/out.txt'},
        'poll': FAST,
    }
    values.update(overrides)
    return WorkflowSpec(**values)


# ---- fake Cromwell --------------------------------------------------------------------------


@dataclass
class FakeWorkflow:
    id: str
    script: list[str]
    outputs: dict[str, Any]
    failures: list[dict[str, Any]] = field(default_factory=list)
    status_calls: int = 0

    def next_status(self) -> str:
        self.status_calls += 1
        if len(self.script) > 1:
            return self.script.pop(0)
        return self.script[0]

    @property
    def current(self) -> str:
        return self.script[0]


class FakeCromwell:
    """
    In-process Cromwell. Each new workflow takes its status script from `scripts` (a queue) or
    `default_script`; each /status call advances it by one; the last status sticks.
    """

    def __init__(self) -> None:
        self.lock = threading.Lock()
        self.workflows: dict[str, FakeWorkflow] = {}
        self.default_script = ['Submitted', 'Running', 'Succeeded']
        self.scripts: list[list[str]] = []
        self.default_outputs: dict[str, Any] = {'Wfl.out': 'gs://exec/out.txt'}
        self.default_failures: list[dict[str, Any]] = []
        self.submissions: list[dict[str, Any]] = []
        self.aborts: list[str] = []
        self.requests: list[tuple[str, str]] = []
        self.fail_next: list[int] = []  # HTTP codes to return for the next N requests
        self.submit_status: int | None = None  # e.g. 400 to reject submissions
        self.server = ThreadingHTTPServer(('127.0.0.1', 0), self._handler())
        self.thread = threading.Thread(target=self.server.serve_forever, kwargs={'poll_interval': 0.02}, daemon=True)

    @property
    def url(self) -> str:
        return f'http://127.0.0.1:{self.server.server_address[1]}'

    def add(self, script: list[str], outputs: dict[str, Any] | None = None, workflow_id: str | None = None) -> str:
        wid = workflow_id or str(uuid.uuid4())
        with self.lock:
            self.workflows[wid] = FakeWorkflow(wid, list(script), dict(outputs or self.default_outputs))
        return wid

    def status_of(self, wid: str) -> str:
        return self.workflows[wid].current

    def _handler(self) -> type[BaseHTTPRequestHandler]:
        fake = self

        class Handler(BaseHTTPRequestHandler):
            def log_message(self, *_args: Any) -> None:
                pass

            def _send(self, code: int, body: Any) -> None:
                data = json.dumps(body).encode()
                self.send_response(code)
                self.send_header('Content-Type', 'application/json')
                self.send_header('Content-Length', str(len(data)))
                self.end_headers()
                self.wfile.write(data)

            def _route(self, method: str) -> None:
                path = self.path.split('?', 1)[0]
                prefix = '/api/workflows/v1'
                with fake.lock:
                    fake.requests.append((method, self.path))
                    if self.headers.get('Authorization') != 'Bearer test-token':
                        self._send(401, {'status': 'error'})
                        return
                    if fake.fail_next:
                        self._send(fake.fail_next.pop(0), {'status': 'error', 'message': 'injected'})
                        return
                    body = b''
                    if 'Content-Length' in self.headers:
                        body = self.rfile.read(int(self.headers['Content-Length']))
                    if method == 'POST' and path == prefix:
                        self._submit(body)
                        return
                    rest = path[len(prefix) + 1 :].split('/')
                    if len(rest) != 2:
                        self._send(404, {'status': 'fail'})
                        return
                    wid, action = rest
                    wf = fake.workflows.get(wid)
                    if wf is None:
                        self._send(404, {'status': 'fail', 'message': f'Unrecognized workflow ID: {wid}'})
                        return
                    if action == 'status':
                        self._send(200, {'id': wid, 'status': wf.next_status()})
                    elif action == 'metadata':
                        self._send(
                            200,
                            {
                                'id': wid,
                                'workflowName': 'Wfl',
                                'status': wf.current,
                                'calls': {},
                                'failures': wf.failures,
                            },
                        )
                    elif action == 'outputs':
                        self._send(200, {'id': wid, 'outputs': wf.outputs})
                    elif action == 'abort' and method == 'POST':
                        fake.aborts.append(wid)
                        if wf.current in ('Succeeded', 'Failed', 'Aborted'):
                            self._send(403, {'status': 'error', 'message': 'terminal'})
                        else:
                            wf.script = ['Aborting', 'Aborted']
                            self._send(200, {'id': wid, 'status': 'Aborting'})
                    else:
                        self._send(404, {'status': 'fail'})

            def _submit(self, body: bytes) -> None:
                if fake.submit_status:
                    self._send(fake.submit_status, {'status': 'fail', 'message': 'rejected'})
                    return
                raw = b'Content-Type: ' + self.headers['Content-Type'].encode() + b'\r\n\r\n' + body
                msg = BytesParser(policy=email_policy).parsebytes(raw)
                parts = {
                    p.get_param('name', header='content-disposition'): p.get_payload(decode=True)
                    for p in msg.iter_parts()
                }
                wid = str(uuid.uuid4())
                script = fake.scripts.pop(0) if fake.scripts else list(fake.default_script)
                fake.workflows[wid] = FakeWorkflow(wid, script, dict(fake.default_outputs), list(fake.default_failures))
                fake.submissions.append({'id': wid, **parts})
                self._send(201, {'id': wid, 'status': 'Submitted'})

            def do_GET(self) -> None:
                self._route('GET')

            def do_POST(self) -> None:
                self._route('POST')

        return Handler

    def __enter__(self) -> Self:
        self.thread.start()
        return self

    def __exit__(self, *exc: object) -> None:
        self.server.shutdown()
        self.server.server_close()


@pytest.fixture
def cromwell() -> Iterator[FakeCromwell]:
    with FakeCromwell() as fake:
        yield fake


def make_context(cromwell: FakeCromwell, storage: Storage, **kwargs: Any) -> RunContext:
    api = CromwellApi(cromwell.url, 'aud', token_provider=StaticTokenProvider('test-token'), timeout=5)
    values: dict[str, Any] = {
        'api': api,
        'storage': storage,
        'secret_reader': lambda _ds, _al: SECRET,
        'abort_poll_seconds': 0.02,
        'abort_confirm_seconds': 2,
    }
    values.update(kwargs)
    return RunContext(**values)


@pytest.fixture
def ctx(cromwell: FakeCromwell, storage: DirStorage, cpg_config: Path) -> RunContext:
    write_bundle(storage)
    storage.put('gs://exec/out.txt', 'hello')
    return make_context(cromwell, storage)
