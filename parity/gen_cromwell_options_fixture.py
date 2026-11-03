#!/usr/bin/env python3
"""
Generate the Cromwell workflow-options parity fixture (NEXTFLOW_PORT_PLAN.md §5.1).

Runs in the parity env: the OLD stack, with Hail, cpg-utils and cpg-flow-gatk-sv installed, e.g.

    ../cpg-flow-gatk-sv/.venv/bin/python parity/gen_cromwell_options_fixture.py

It calls the real `cpg_utils.cromwell.run_cromwell_workflow` against a fake BashJob and records the
workflow options and inputs JSON it echoes, and drives `cpg_flow_gatk_sv.utils.add_gatk_sv_jobs`
far enough to record how it serialises inputs. The secret is a fake placeholder key.
test/test_cromwell.py (Hail-free) compares `gatk_sv_nf.cromwell.workflow_options` against the result.

Regenerate only when pinning a new cpg-utils.
"""

import contextlib
import importlib.metadata
import json
import os
import re
import sys
import tempfile
from pathlib import Path
from types import SimpleNamespace
from typing import Any

OUT = Path(__file__).resolve().parent.parent / 'test' / 'fixtures' / 'cromwell_options_parity.json'

FAKE_SECRET = json.dumps(
    {
        'type': 'service_account',
        'client_email': 'fixture-cromwell@fixture-project.iam.gserviceaccount.com',
        'private_key': 'PLACEHOLDER-NOT-A-KEY',
    },
)

CONFIG_TOML = """
[workflow]
dataset = "fixture-ds"
access_level = "test"
dataset_gcp_project = "fixture-project"
driver_image = "fixture-driver-image"
gatk_sv_commit = "0123456789abcdef0123456789abcdef01234567"

[storage.default]
default = "gs://cpg-fixture-ds-test"
analysis = "gs://cpg-fixture-ds-test-analysis"
tmp = "gs://cpg-fixture-ds-test-tmp"
web = "gs://cpg-fixture-ds-test-web"

[storage.fixture-ds]
default = "gs://cpg-fixture-ds-test"
analysis = "gs://cpg-fixture-ds-test-analysis"
tmp = "gs://cpg-fixture-ds-test-tmp"
web = "gs://cpg-fixture-ds-test-web"

[storage.other-ds]
default = "gs://cpg-other-ds-main"
analysis = "gs://cpg-other-ds-main-analysis"
tmp = "gs://cpg-other-ds-main-tmp"
web = "gs://cpg-other-ds-main-web"
"""

# each case: kwargs for run_cromwell_workflow (minus job/workflow/cwd/libs/input_dict)
CASES: list[dict[str, Any]] = [
    {
        'name': 'with_ar_guid',
        'dataset': 'fixture-ds',
        'access_level': 'test',
        'output_prefix': 'gatk_sv/output/FormatVcfForGatk/fixture-ds',
        'labels': {'stage': 'formatvcfforgatk'},
        'project': None,
        'copy_outputs_to_gcp': False,
        'ar_guid_override': '6b1c8f1e-7d0a-4a7e-9a8e-3f2b0d1c4e5f',
    },
    {
        'name': 'without_ar_guid',
        'dataset': 'fixture-ds',
        'access_level': 'test',
        'output_prefix': 'gatk_sv/output/EvidenceQC/fixture-ds',
        'labels': {'stage': 'evidenceqc'},
        'project': None,
        'copy_outputs_to_gcp': False,
        'ar_guid_override': None,
    },
    {
        'name': 'copy_outputs_explicit_project_gse_labels',
        'dataset': 'other-ds',
        'access_level': 'standard',
        'output_prefix': 'gatk_sv/output/GatherSampleEvidence/other-ds/cpgaaa111',
        'labels': {'stage': 'gathersampleevidence', 'dataset': 'other-ds', 'sequencing-group': 'cpgaaa111'},
        'project': 'billing-project',
        'copy_outputs_to_gcp': True,
        'ar_guid_override': 'abc-123',
    },
    {
        'name': 'no_labels',
        'dataset': 'fixture-ds',
        'access_level': 'full',
        'output_prefix': 'x/y',
        'labels': None,
        'project': None,
        'copy_outputs_to_gcp': True,
        'ar_guid_override': None,
    },
]

# inputs for add_gatk_sv_jobs; tagged so the JSON fixture can carry Paths and sets
INPUT_CASES: list[dict[str, Any]] = [
    {
        'wfl_name': 'GatherSampleEvidence',
        'inputs': {
            'bam_or_cram_file': {'__path__': 'gs://cpg-fixture-ds-test/cram/CPGAAA111.cram'},
            'sample_id': 'CPGAAA111',
            'collect_coverage': True,
            'run_module_metrics': False,
            'reference_fasta': {'__path__': 'gs://refs/hg38.fasta'},
            'primary_contigs_list': {'__path__': 'gs://refs/contigs.list'},
            'disk_size_gb': 200,
            'mem_gib': 7.5,
            'samples': ['CPGAAA111', 'CPGBBB222'],
            'int_list': [1, 2, 3],
            'path_list': [{'__path__': 'gs://a/b.vcf'}, {'__path__': 'gs://a/c.vcf'}],
            'one_set': {'__set__': ['only']},
            'nested': {'k': [1, 2], 'p': 'gs://x'},
            'none_value': None,
        },
    },
]


def decode(value: Any) -> Any:
    from cpg_utils import to_path

    if isinstance(value, dict) and set(value) == {'__path__'}:
        return to_path(value['__path__'])
    if isinstance(value, dict) and set(value) == {'__set__'}:
        return set(value['__set__'])
    if isinstance(value, dict):
        return {k: decode(v) for k, v in value.items()}
    if isinstance(value, list):
        return [decode(v) for v in value]
    return value


class FakeBashJob:
    def __init__(self) -> None:
        self.commands: list[str] = []
        self.out_workflow_id = 'FAKE_RESOURCE'

    def command(self, cmd: str) -> None:
        self.commands.append(cmd)


def echoed(job: FakeBashJob, target: str) -> str:
    pattern = re.compile(r"echo '(.*)' > " + re.escape(target), re.DOTALL)
    for cmd in job.commands:
        if m := pattern.search(cmd):
            return m.group(1)
    raise AssertionError(f'no echo to {target} in {job.commands}')


def main() -> None:
    with tempfile.NamedTemporaryFile('w', suffix='.toml', delete=False) as fh:
        fh.write(CONFIG_TOML)
        config_path = fh.name
    os.environ['CPG_CONFIG_PATH'] = config_path

    from cpg_utils import config as cpg_config

    cpg_config.set_config_paths([config_path])

    import cpg_utils.cromwell as old

    old.read_secret = lambda _project, _name: FAKE_SECRET  # the only network call

    cases = []
    for case in CASES:
        job = FakeBashJob()
        input_dict = {'Wfl.some_input': 'gs://x/y', 'Wfl.n': 3}
        old.run_cromwell_workflow(
            job=job,
            dataset=case['dataset'],
            access_level=case['access_level'],
            workflow='Wfl.wdl',
            cwd='wdl',
            libs=['.'],
            output_prefix=case['output_prefix'],
            labels=case['labels'],
            input_dict=input_dict,
            project=case['project'],
            copy_outputs_to_gcp=case['copy_outputs_to_gcp'],
            ar_guid_override=case['ar_guid_override'],
        )
        cases.append(
            {
                **case,
                'input_dict': input_dict,
                'options_json': echoed(job, 'workflow-options.json'),
                'inputs_json': echoed(job, '$TMPDIR/inputs.json'),
            },
        )

    # add_gatk_sv_jobs: capture the input_dict it hands to cpg_utils.cromwell, then stop
    import cpg_utils.hail_batch

    cpg_utils.hail_batch.get_batch = lambda *a, **k: object()
    from cpg_flow_gatk_sv import utils as gsv_utils

    class Stop(Exception):
        pass

    captured: dict[str, Any] = {}

    def capture(**kwargs: Any) -> None:
        captured.update(kwargs)
        raise Stop

    old.run_cromwell_workflow_from_repo_and_get_outputs = capture

    input_cases = []
    for icase in INPUT_CASES:
        captured.clear()
        with contextlib.suppress(Stop):
            gsv_utils.add_gatk_sv_jobs(
                dataset=SimpleNamespace(name='fixture-ds'),
                wfl_name=icase['wfl_name'],
                input_dict=decode(icase['inputs']),
                expected_out_dict={},
            )
        input_cases.append({**icase, 'expected_input_dict': captured['input_dict']})

    fixture = {
        'generated_by': 'parity/gen_cromwell_options_fixture.py',
        'cpg_utils_version': importlib.metadata.version('cpg-utils'),
        'cpg_flow_gatk_sv_version': importlib.metadata.version('cpg-flow-gatk-sv'),
        'config_toml': CONFIG_TOML,
        'secret_json': FAKE_SECRET,
        'cases': cases,
        'input_cases': input_cases,
    }
    OUT.parent.mkdir(parents=True, exist_ok=True)
    OUT.write_text(json.dumps(fixture, indent=2) + '\n')
    print(f'wrote {OUT}', file=sys.stderr)


if __name__ == '__main__':
    main()
