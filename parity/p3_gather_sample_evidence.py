#!/usr/bin/env python3
"""
P3, old side, for GatherSampleEvidence (NEXTFLOW_PORT_PLAN.md §10).

Runs in the parity env: the OLD stack, with cpg-flow-gatk-sv installed, e.g.

    ../cpg-flow-gatk-sv/.venv/bin/python parity/p3_gather_sample_evidence.py \\
        --plan <plan_dir> --chunk <chunk.json> --ar-guid <guid> [--existing existing.json] --out old.json

For each SG in the chunk this runs cpg-flow-gatk-sv's own `GatherSampleEvidence.expected_outputs` and
`create_gather_sample_evidence_jobs`, through the real `utils.add_gatk_sv_jobs`. The call it makes to
`cpg_utils.cromwell.run_cromwell_workflow_from_repo_and_get_outputs` is recorded, and its arguments are passed to
the real `run_cromwell_workflow` with a fake job, so the inputs JSON and workflow options recorded are the ones the
old pipeline would submit. Nothing is submitted. Patched: `hail_batch.get_batch`, `read_secret` (fake secret),
`randint` (lower bound: the per-size bounds before jitter) and, with --existing, the Manta VCF existence check.

The output has the shape of `nf-gatk-sv stage GatherSampleEvidence --dump-inputs`; compare the two with
parity/p3_compare.py.

Each SG is run with a freshly loaded config. In cpg-flow-gatk-sv, `only_jobs.append('manta')` mutates the cached
config, so once one SG has no Manta VCF to reuse, every later SG in the same run also runs Manta. The port doesn't
carry that over, so a run with mixed Manta presence would differ here by SG order, not by logic. See §9.
"""

import argparse
import json
import os
import re
import sys
import tempfile
from pathlib import Path
from types import SimpleNamespace
from typing import Any

STAGE = 'GatherSampleEvidence'
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


def capture_sg(sg_data: dict[str, Any], config_paths: list[str], existing: set[str] | None) -> dict[str, Any]:
    load_config(config_paths)

    import cpg_utils.cromwell as old_cromwell
    import cpg_utils.hail_batch
    from cpg_flow_gatk_sv import utils as gsv_utils
    from cpg_flow_gatk_sv.jobs import GatherSampleEvidence as gse_job
    from cpg_flow_gatk_sv.singlesample_workflow import GatherSampleEvidence
    from cpg_utils import to_path

    cpg_utils.hail_batch.get_batch = lambda *_a, **_k: FakeBatch()
    gsv_utils.hail_batch.get_batch = cpg_utils.hail_batch.get_batch
    old_cromwell.read_secret = lambda _project, _name: FAKE_SECRET
    gsv_utils.randint = lambda low, _high: low

    if existing is not None:

        class _Exists:
            """to_path() whose exists() answers from --existing; everything else is the real path."""

            def __init__(self, path: Any) -> None:
                self._path = to_path(path)

            def exists(self) -> bool:
                return str(self._path) in existing

            def __getattr__(self, name: str) -> Any:
                return getattr(self._path, name)

        gse_job.to_path = _Exists
    else:
        gse_job.to_path = to_path

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

    sg = make_sg(sg_data, {})
    # the stage class is behind cpg-flow's @stage decorator; expected_outputs only reads self.name
    expected = GatherSampleEvidence.__wrapped__.expected_outputs(SimpleNamespace(name=STAGE), sg)
    gse_job.create_gather_sample_evidence_jobs(sg=sg, expected_outputs=expected)
    # the job pops the reused Manta VCF out of `expected` in place, so this is what add_gatk_sv_jobs collected
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


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument('--plan', required=True, help='plan dir holding config.toml and manifest.json')
    parser.add_argument('--chunk', required=True, help='JSON list of SG IDs')
    parser.add_argument('--ar-guid', required=True)
    parser.add_argument('--existing', help='JSON list of gs:// paths to treat as existing; default: check GCS')
    parser.add_argument('--out', required=True)
    args = parser.parse_args()

    plan = args.plan.rstrip('/')
    # the old jobs read the GUID from config (try_get_ar_guid); the port takes it on the command line
    with tempfile.NamedTemporaryFile('w', suffix='.toml', delete=False) as fh:
        fh.write(f'[workflow]\n"ar-guid" = "{args.ar_guid}"\n')
        guid_layer = fh.name
    config_paths = [f'{plan}/config.toml', guid_layer]

    manifest = json.loads(read_text(f'{plan}/manifest.json'))
    sgs = {sg['id']: sg for sg in manifest['sequencing_groups']}
    existing = set(json.loads(Path(args.existing).read_text())) if args.existing else None

    dump = {sg_id: capture_sg(sgs[sg_id], config_paths, existing) for sg_id in json.loads(read_text(args.chunk))}
    Path(args.out).write_text(json.dumps(dump, indent=2, sort_keys=True))
    print(f'wrote {args.out} ({len(dump)} SGs)', file=sys.stderr)
    return 0


if __name__ == '__main__':
    sys.exit(main())
