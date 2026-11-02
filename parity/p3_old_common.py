"""
P3 old side, shared by every stage (NEXTFLOW_PORT_PLAN.md §10). Runs in the parity env, with cpg-flow-gatk-sv.

`patch_old_stack()` stubs Hail Batch and the secret, pins `randint` to its lower bound (the per-size polling bounds
before jitter), and records the call that `utils.add_gatk_sv_jobs` makes to
`cpg_utils.cromwell.run_cromwell_workflow_from_repo_and_get_outputs`, passing its arguments to the real
`run_cromwell_workflow` with a fake job. `capture()` turns the recording into the shape of
`nf-gatk-sv stage <Stage> --dump-inputs`, for parity/p3_compare.py.
"""

import json
import os
import re
from typing import Any

FAKE_SECRET = json.dumps(
    {'type': 'service_account', 'client_email': 'p3-cromwell@p3.iam.gserviceaccount.com', 'private_key': 'NOT-A-KEY'},
)


class FakeBashJob:
    """Stands in for both the submit job (records commands) and the copy job."""

    def __init__(self) -> None:
        self.commands: list[str] = []
        self.out_workflow_id = 'FAKE_RESOURCE'

    def command(self, cmd: str) -> None:
        self.commands.append(cmd)

    def image(self, _image: str) -> None:
        pass


class FakeBatch:
    def new_job(self, *_a: Any, **_k: Any) -> FakeBashJob:
        return FakeBashJob()

    new_bash_job = new_job


def echoed(job: FakeBashJob, target: str) -> str:
    pattern = re.compile(r"echo '(.*)' > " + re.escape(target), re.DOTALL)
    for cmd in job.commands:
        if m := pattern.search(cmd):
            return m.group(1)
    raise AssertionError(f'no echo to {target} in {job.commands}')


def read_text(uri: str) -> str:
    from cpg_utils import to_path

    return to_path(uri).read_text()


def load_config(config_paths: list[str]) -> None:
    from cpg_utils import config as cpg_config

    os.environ['CPG_CONFIG_PATH'] = ','.join(config_paths)
    cpg_config.set_config_paths(config_paths)  # also drops the cached config


def make_sg(data: dict[str, Any], datasets: dict[str, Any]) -> Any:
    from cpg_flow import targets
    from cpg_flow.filetypes import CramPath
    from cpg_utils.config import config_retrieve

    ds = datasets.setdefault(data['dataset'], targets.Dataset(data['dataset']))
    sg = ds.add_sequencing_group(
        data['id'],
        sequencing_type=config_retrieve(['workflow', 'sequencing_type']),
        sequencing_technology='short-read',
        sequencing_platform='illumina',
        external_id=data.get('external_id'),
        participant_id=data.get('participant_id'),
    )
    sg.cram = CramPath(data['cram']) if data.get('cram') else None
    sg.active = data.get('active', True)
    return sg


def patch_old_stack() -> dict[str, Any]:
    """Patch the old stack (see the module docstring). Returns the dict each submission is recorded into."""
    import cpg_utils.cromwell as old_cromwell
    import cpg_utils.hail_batch
    from cpg_flow_gatk_sv import utils as gsv_utils

    cpg_utils.hail_batch.get_batch = lambda *_a, **_k: FakeBatch()
    gsv_utils.hail_batch.get_batch = cpg_utils.hail_batch.get_batch
    old_cromwell.read_secret = lambda _project, _name: FAKE_SECRET
    gsv_utils.randint = lambda low, _high: low

    recorded: dict[str, Any] = {}

    def record(**kwargs: Any) -> tuple[FakeBashJob, dict[str, str]]:
        job = FakeBashJob()
        old_cromwell.run_cromwell_workflow(
            job=job,
            dataset=kwargs['dataset'],
            access_level=old_cromwell.get_access_level(),
            workflow=kwargs['workflow'],
            cwd=kwargs['cwd'],
            libs=kwargs['libs'],
            output_prefix=kwargs['output_prefix'],
            input_dict=kwargs['input_dict'],
            labels=kwargs['labels'],
            copy_outputs_to_gcp=kwargs['copy_outputs_to_gcp'],
        )
        recorded.update(kwargs, options_json=echoed(job, 'workflow-options.json'))
        recorded['inputs_json'] = echoed(job, '$TMPDIR/inputs.json')
        return job, {key: f'resource-{key}' for key in kwargs['outputs_to_collect']}

    gsv_utils.cromwell.run_cromwell_workflow_from_repo_and_get_outputs = record

    return recorded


def capture(recorded: dict[str, Any], expected: dict[str, Any]) -> dict[str, Any]:
    """The recorded submission, plus what add_gatk_sv_jobs was told to collect (`expected`, as passed in)."""
    from cpg_utils import cromwell as old_cromwell

    collect = {
        key: [str(p) for p in value] if isinstance(value, list) else str(value) for key, value in expected.items()
    }
    options = json.loads(recorded['options_json'])
    options['user_service_account_json'] = '<redacted>'
    return {
        'inputs': json.loads(recorded['inputs_json']),
        'options': options,
        'collect': collect,
        'outputs_to_collect': {key: str(v) for key, v in recorded['outputs_to_collect'].items()},
        'workflow': recorded['workflow'],
        'dataset': recorded['dataset'],
        'access_level': old_cromwell.get_access_level(),
        'output_prefix': recorded['output_prefix'],
        'copy_outputs': recorded['copy_outputs_to_gcp'],
        'poll': {'min': recorded['min_watch_poll_interval'], 'max': recorded['max_watch_poll_interval']},
        'time_limit_seconds': recorded['time_limit_seconds'],
        'source': {
            'repo': recorded['repo'],
            'commit': recorded['commit'],
            'cwd': recorded['cwd'],
            'libs': recorded['libs'],
        },
    }


def guid_config_paths(plan: str, ar_guid: str) -> list[str]:
    """The plan's config plus a layer with the GUID: the old jobs read it from config (try_get_ar_guid)."""
    import tempfile

    with tempfile.NamedTemporaryFile('w', suffix='.toml', delete=False) as fh:
        fh.write(f'[workflow]\n"ar-guid" = "{ar_guid}"\n')
    return [f'{plan.rstrip("/")}/config.toml', fh.name]
