"""
Fixtures for gatk_sv_nf: a resolved config, directory-backed "GCS", and an in-memory Cromwell for
`cpg_utils.cromwell_client`.

`make_context` gives a `RunContext` that never touches the real services, through the client's own arguments:
the fake Cromwell, polling that never waits, a fake service-account key instead of Secret Manager, and output
copies done inside the `storage` fixture's directory instead of on GCS.
"""

import io
import json
import threading
import zipfile
from collections.abc import Callable, Iterator
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import TYPE_CHECKING, Any

import pytest

from gatk_sv_nf.storage import LocalBackend, Storage
from gatk_sv_nf.wdl_bundle import BundleManifest

if TYPE_CHECKING:
    from gatk_sv_nf.cromwell import RunContext

COMMIT = '890582922bccb4b651275506a34311c949417f6c'
FASTA = 'gs://cpg-common-main/references/hg38/Homo_sapiens_assembly38_masked.fasta'
SECRET = json.dumps({'type': 'service_account', 'client_email': 'sa@example.com', 'private_key': 'PLACEHOLDER'})
CREATED_AT = '2026-11-03T00:00:00+00:00'


def _storage(ds: str) -> str:
    return f"""
[storage.{ds}]
default = "gs://cpg-{ds}-test"
analysis = "gs://cpg-{ds}-test-analysis"
tmp = "gs://cpg-{ds}-test-tmp"
web = "gs://cpg-{ds}-test-web"
web_url = "https://test-web.populationgenomics.org.au/{ds}"
"""


BASE_CONFIG = (
    f"""
[workflow]
dataset = "analysis-ds"
access_level = "test"
dataset_gcp_project = "analysis-ds-project"
sequencing_type = "genome"
check_expected_outputs = true
gatk_sv_commit = "{COMMIT}"
ref_fasta = "{FASTA}"
time_limit_seconds = 86400

[images]
cloud_sdk_docker = "img/cloud_sdk"
gatk_docker = "img/gatk"
gatk_docker_pesr_override = "img/gatk_pesr"
genomes_in_the_cloud_docker = "img/gitc"
manta_docker = "img/manta"
samtools_cloud_docker = "img/samtools"
scramble_docker = "img/scramble"
sv_base_mini_docker = "img/sv_base_mini"
sv_pipeline_docker = "img/sv_pipeline"
wham_docker = "img/wham"

[references]
manta_region_bed = "gs://ref/manta.bed.gz"
manta_region_bed_index = "gs://ref/manta.bed.gz.tbi"
mei_bed = "gs://ref/mei.bed"
preprocessed_intervals = "gs://ref/intervals.list"
primary_contigs_fai = "gs://ref/contigs.fai"
primary_contigs_list = "gs://ref/contigs.list"
dbsnp_vcf = "gs://ref/dbsnp.vcf"
wham_include_list_bed_file = "gs://ref/wham.bed"

[resource_overrides.GatherSampleEvidence.runtime_attr_wham]
mem_gb = 32

[cromwell_polling_intervals.large]
min = 1
max = 2
"""
    + _storage('default')
    + _storage('analysis-ds')
    + _storage('fixture-ds')
)


@pytest.fixture
def write_config(tmp_path: Path) -> Iterator:
    """Call with extra TOML to append; returns the config path. Resets cpg_utils' cache afterwards."""
    from cpg_utils import config as cpg_config_module

    def write(extra: str = '') -> Path:
        path = tmp_path / 'config.toml'
        path.write_text(BASE_CONFIG + extra)
        cpg_config_module.set_config_paths([str(path)])
        return path

    yield write
    cpg_config_module._config_paths = []
    cpg_config_module._config = None


@pytest.fixture
def config(write_config) -> Path:
    return write_config()


# ---- storage ------------------------------------------------------------------------------------------------------


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


def write_bundle(storage: Storage, *, bundle: str, workflow: str, verified: bool = True) -> None:
    """A minimal gatk-sv bundle, as `nf-gatk-sv prepare-wdl` leaves it."""
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, 'w') as zf:
        zf.writestr('Structs.wdl', 'version 1.0\n')
    root = bundle.rstrip('/') + '/'
    storage.write(root + 'deps.zip', buf.getvalue())
    storage.write(root + workflow, f'version 1.0\nworkflow {workflow.removesuffix(".wdl")} {{}}\n')
    manifest = BundleManifest('gatk-sv', 'populationgenomics', COMMIT, 'wdl', ['.'], verified, CREATED_AT)
    storage.write(root + 'bundle.json', manifest.to_json())


# ---- fake Cromwell ------------------------------------------------------------------------------------------------


class FakeCromwell:
    """
    An in-memory stand-in for `cpg_utils.cromwell_client.CromwellClient`, safe to call from many threads.

    Each submitted workflow takes its status script from `script_for(prefixed inputs)`, which defaults to
    `default_script`. Each `status()` call advances the script by one; the last status sticks. Every workflow
    reports `default_outputs` (or `outputs_for(inputs)`).
    """

    def __init__(self) -> None:
        self.lock = threading.Lock()
        self.workflows: dict[str, dict[str, Any]] = {}
        self.default_script = ['Submitted', 'Running', 'Succeeded']
        self.script_for: Callable[[dict[str, Any]], list[str]] = lambda _inputs: list(self.default_script)
        self.default_outputs: dict[str, Any] = {}
        self.failures: list[dict[str, Any]] = [{'message': 'task failed', 'causedBy': [{'message': 'exit 1'}]}]
        self.submissions: list[dict[str, Any]] = []
        self.aborts: list[str] = []
        self.submission_age_seconds = 0  # backdates `submission`, e.g. to pass a time limit

    def submit(
        self,
        workflow_source: bytes,
        *,
        workflow_filename: str = 'workflow.wdl',
        input_documents: Any = (),
        options: dict[str, Any],
        dependencies: bytes | None = None,
        labels: dict[str, str] | None = None,
    ) -> str:
        inputs: dict[str, Any] = {}
        for document in input_documents:
            inputs.update(json.loads(document))
        with self.lock:
            wid = f'wf-{len(self.submissions) + 1}'
            self.workflows[wid] = {
                # the client's time limit counts from this
                'submission': (datetime.now(UTC) - timedelta(seconds=self.submission_age_seconds)).isoformat(),
                'script': self.script_for(inputs),
                'labels': dict(labels or {}),
                'outputs': dict(self.default_outputs),
            }
            self.submissions.append(
                {
                    'id': wid,
                    'workflow_source': workflow_source,
                    'workflow_filename': workflow_filename,
                    'inputs': inputs,
                    'options': options,
                    'dependencies': dependencies,
                    'labels': dict(labels or {}),
                }
            )
        return wid

    def _workflow(self, wid: str) -> dict[str, Any]:
        from cpg_utils.cromwell_client import CromwellNotFound

        if wid not in self.workflows:
            raise CromwellNotFound(f'Unrecognized workflow ID: {wid}')
        return self.workflows[wid]

    def status(self, wid: str) -> str:
        with self.lock:
            script = self._workflow(wid)['script']
            return script.pop(0) if len(script) > 1 else script[0]

    def current(self, wid: str) -> str:
        return self.workflows[wid]['script'][0]

    def metadata(self, wid: str, *, include_keys: Any = ()) -> dict[str, Any]:
        with self.lock:
            wf = self._workflow(wid)
            status = wf['script'][0]
        return {
            'id': wid,
            'workflowName': 'Wfl',
            'status': status,
            'submission': wf['submission'],
            'calls': {},
            'failures': self.failures if status == 'Failed' else [],
        }

    def outputs(self, wid: str) -> dict[str, Any]:
        with self.lock:
            return dict(self._workflow(wid)['outputs'])

    def abort(self, wid: str) -> str:
        with self.lock:
            self.aborts.append(wid)
            self._workflow(wid)['script'] = ['Aborting', 'Aborted']
        return 'Aborting'

    def query_by_label(self, key: str, value: str) -> list[dict[str, Any]]:
        with self.lock:
            return [
                {'id': wid, 'status': wf['script'][0], 'submission': wf['submission'], 'end': wf['submission']}
                for wid, wf in self.workflows.items()
                if wf['labels'].get(key) == value
            ]


@pytest.fixture
def cromwell() -> FakeCromwell:
    return FakeCromwell()


def make_context(cromwell: FakeCromwell, storage: Storage) -> RunContext:
    from cpg_utils.cromwell_client import Polling

    from gatk_sv_nf.cromwell import RunContext

    def copy_file(src: str, dst: str) -> None:
        blob = storage.read(src)
        if blob is None:
            raise FileNotFoundError(src)
        storage.write(dst, blob.data)

    return RunContext(
        storage=storage,
        client=cromwell,
        polling=Polling(min_seconds=0, max_seconds=0),
        service_account_json=SECRET,
        copy_file=copy_file,
    )
