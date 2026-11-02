#!/usr/bin/env python3
"""
P3 for GatherSampleEvidence, offline, over the §9 variants (NEXTFLOW_PORT_PLAN.md §10).

    uv run python parity/p3_gse_offline.py [--old-python ../cpg-flow-gatk-sv/.venv/bin/python] [--keep DIR]

For each variant this writes a plan dir (config.toml, manifest.json, a chunk and the paths to treat as existing),
then runs the old side (parity/p3_gather_sample_evidence.py, in the cpg-flow-gatk-sv env) and the new side
(parity/p3_new_side.py, the nf-gatk-sv CLI's --dump-inputs) and compares them (parity/p3_compare.py).

Each config.toml is cpg-flow-gatk-sv's own config_template.toml, so images, references and resource overrides
are the production values, plus the keys analysis-runner would inject and the variant's overrides. Nothing
touches GCS, metamist or Cromwell. This checks the logic. A run against a real `nf-gatk-sv plan` dir, with real
existence checks, is still needed once `plan` exists.
"""

import argparse
import copy
import json
import subprocess
import sys
import tempfile
from pathlib import Path
from typing import Any

import toml

REPO = Path(__file__).resolve().parent.parent
OLD_REPO = REPO.parent / 'cpg-flow-gatk-sv'
TEMPLATE = OLD_REPO / 'src' / 'cpg_flow_gatk_sv' / 'config_template.toml'
AR_GUID = 'p3-0000-1111-2222-333344445555'

SGS = [
    {'id': 'CPG111', 'dataset': 'ds-one', 'cram': 'gs://cpg-ds-one-{ns}/cram/CPG111.cram', 'participant_id': 'P1'},
    {'id': 'CPG222', 'dataset': 'ds-one', 'cram': 'gs://cpg-ds-one-{ns}/cram/CPG222.cram', 'external_id': 'S2'},
    {'id': 'CPG333', 'dataset': 'ds-two', 'cram': 'gs://cpg-ds-two-{ns}/cram/CPG333.cram'},
]
DATASETS = ['analysis-ds', 'ds-one', 'ds-two']


def storage(access_level: str) -> dict[str, dict[str, str]]:
    ns = 'test' if access_level == 'test' else 'main'
    sections = {}
    for ds in ['default', *DATASETS]:
        name = DATASETS[0] if ds == 'default' else ds
        sections[ds] = {
            'default': f'gs://cpg-{name}-{ns}',
            'analysis': f'gs://cpg-{name}-{ns}-analysis',
            'tmp': f'gs://cpg-{name}-{ns}-tmp',
            'web': f'gs://cpg-{name}-{ns}-web',
            'web_url': f'https://{ns}-web.populationgenomics.org.au/{name}',
        }
    return sections


def manta(sg_id: str, dataset: str, ns: str, subdir: str = '') -> str:
    return f'gs://cpg-{dataset}-{ns}/{subdir}sv_evidence/{sg_id}.manta.vcf.gz'


# name -> (config overrides, paths that exist: fn(ns) -> list)
VARIANTS: dict[str, tuple[dict[str, Any], Any]] = {
    'default': ({}, lambda _ns: []),
    'standard_access': ({'workflow': {'access_level': 'standard'}}, lambda _ns: []),
    'exome': ({'workflow': {'sequencing_type': 'exome'}}, lambda _ns: []),
    'only_manta': ({'workflow': {'GatherSampleEvidence': {'only_jobs': ['manta']}}}, lambda _ns: []),
    'only_wham_pesr': ({'workflow': {'GatherSampleEvidence': {'only_jobs': ['wham', 'pesr']}}}, lambda _ns: []),
    'only_evidence': (
        {'workflow': {'GatherSampleEvidence': {'only_jobs': ['coverage_counts', 'pesr']}}},
        lambda _ns: [],
    ),
    # scramble without manta: CPG111 reuses its Manta VCF, CPG222 and CPG333 have none, so run Manta too
    'scramble_mixed_manta': (
        {'workflow': {'GatherSampleEvidence': {'only_jobs': ['scramble']}}},
        lambda ns: [manta('CPG111', 'ds-one', ns), manta('CPG111', 'ds-one', ns) + '.tbi'],
    ),
    'overrides_copy_polling_time': (
        {
            'workflow': {'copy_outputs': True, 'time_limit_seconds': 3600},
            'resource_overrides': {'GatherSampleEvidence': {'run_module_metrics': False, 'extra_input': 7}},
            'cromwell_polling_intervals': {'large': {'min': 11, 'max': 99}},
        },
        lambda _ns: [],
    ),
}


def deep_merge(base: dict, overlay: dict) -> dict:
    out = copy.deepcopy(base)
    for key, value in overlay.items():
        if isinstance(value, dict) and isinstance(out.get(key), dict):
            out[key] = deep_merge(out[key], value)
        else:
            out[key] = copy.deepcopy(value)
    return out


def write_plan(plan: Path, overrides: dict[str, Any], existing_fn: Any) -> tuple[Path, Path]:
    config = toml.loads(TEMPLATE.read_text())
    config = deep_merge(
        config,
        {
            'workflow': {
                'dataset': DATASETS[0],
                'access_level': 'test',
                'dataset_gcp_project': 'analysis-ds-project',
                'driver_image': 'p3-driver-image',
            },
        },
    )
    config = deep_merge(config, overrides)
    access_level = config['workflow']['access_level']
    ns = 'test' if access_level == 'test' else 'main'
    config['storage'] = storage(access_level)
    seq_type = config['workflow']['sequencing_type']
    subdir = '' if seq_type == 'genome' else f'{seq_type}/'

    plan.mkdir(parents=True)
    (plan / 'config.toml').write_text(toml.dumps(config))
    sgs = [{**sg, 'cram': sg['cram'].format(ns=ns), 'active': True} for sg in SGS]
    (plan / 'manifest.json').write_text(json.dumps({'sequencing_groups': sgs}, indent=2))
    chunk = plan / 'gse_chunks' / 'chunk_0.json'
    chunk.parent.mkdir()
    chunk.write_text(json.dumps([sg['id'] for sg in sgs]))
    existing = plan.parent / f'{plan.name}.existing.json'
    paths = existing_fn(ns)
    if subdir:
        paths = [p.replace('/sv_evidence/', f'/{subdir}sv_evidence/') for p in paths]
    existing.write_text(json.dumps(paths))
    return chunk, existing


def run(cmd: list[str]) -> None:
    result = subprocess.run(cmd, capture_output=True, text=True, check=False)
    if result.returncode != 0:
        print(f'FAILED ({result.returncode}): {" ".join(cmd)}\n{result.stdout}\n{result.stderr}', file=sys.stderr)
        raise SystemExit(result.returncode)


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument('--old-python', default=str(OLD_REPO / '.venv' / 'bin' / 'python'))
    parser.add_argument('--keep', help='write the plans and dumps here instead of a temp dir')
    parser.add_argument('variants', nargs='*', help=f'default: all of {", ".join(VARIANTS)}')
    args = parser.parse_args()

    root = Path(args.keep) if args.keep else Path(tempfile.mkdtemp(prefix='p3-gse-'))
    failed = []
    for name in args.variants or list(VARIANTS):
        overrides, existing_fn = VARIANTS[name]
        plan = root / name
        chunk, existing = write_plan(plan, overrides, existing_fn)
        old_out, new_out = root / f'{name}.old.json', root / f'{name}.new.json'
        common = ['--existing', str(existing)]
        run([
            args.old_python, '-W', 'ignore', str(REPO / 'parity' / 'p3_gather_sample_evidence.py'),
            '--plan', str(plan), '--chunk', str(chunk), '--ar-guid', AR_GUID, *common, '--out', str(old_out),
        ])  # fmt: skip
        run([
            sys.executable, str(REPO / 'parity' / 'p3_new_side.py'), *common, '--',
            'stage', 'GatherSampleEvidence', '--plan', str(plan), '--chunk', str(chunk), '--ar-guid', AR_GUID,
            '--dump-inputs', str(new_out),
        ])  # fmt: skip
        print(f'== {name}', flush=True)
        result = subprocess.run(
            [sys.executable, str(REPO / 'parity' / 'p3_compare.py'), str(old_out), str(new_out)], check=False
        )
        if result.returncode != 0:
            failed.append(name)

    print(f'\nP3 GatherSampleEvidence: {len(failed)} of {len(args.variants or VARIANTS)} variant(s) differ', end='')
    print(f': {", ".join(failed)}' if failed else '')
    print(f'plans and dumps in {root}')
    return 1 if failed else 0


if __name__ == '__main__':
    sys.exit(main())
