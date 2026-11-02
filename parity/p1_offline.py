#!/usr/bin/env python3
"""
P1, manifest parity, offline (NEXTFLOW_PORT_PLAN.md §10).

    uv run python parity/p1_offline.py [--old-python ../cpg-flow-gatk-sv/.venv/bin/python] [--keep DIR]

Both sides read the same canned metamist (test/fake_metamist.py): the old side is cpg-flow's own
`get_multicohort()` plus cpg-flow-gatk-sv's helpers (parity/p1_old_side.py, in cpg-flow-gatk-sv's env); the new side
is `gatk_sv_nf.manifest.build_manifest`. Compared per variant: the multicohort name, output_version, cohorts and
their order, datasets and their order, active SG IDs, every SG (CRAM, IDs, meta, PED row, job attributes) in
order, the previous spicy VCF, the combined PEDs, the cohort-overlap check, and refusals.

This checks the logic against cpg-flow's. The live P1, against real metamist, is still needed: it checks the data
(the fixture can't show a query returning something unexpected).
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
sys.path.insert(0, str(REPO / 'test'))
OLD_REPO = REPO.parent / 'cpg-flow-gatk-sv'

from fake_metamist import FakeMetamist, fixture  # noqa: E402

from gatk_sv_nf import config as cfg  # noqa: E402
from gatk_sv_nf.manifest import ManifestRequest, build_manifest, check_for_cohort_overlaps, combined_ped  # noqa: E402
from gatk_sv_nf.metamist_client import Metamist  # noqa: E402

REF_PED = 'HG00096\tHG00096\t0\t0\t1\t0\nHG00129\tHG00129\t0\t0\t2\t0\n'
DATASETS = ['analysis-ds', 'ds-one', 'ds-two']


def _overlap(data: dict) -> dict:
    data['cohorts']['COH2']['sequencing_groups'].append('CPG10')
    return data


def _inactive(data: dict) -> dict:
    data['cohorts']['COH2']['status'] = 'archived'
    return data


# name -> (namespace, workflow overrides, fixture edit)
VARIANTS: dict[str, tuple[str, dict[str, Any], Any]] = {
    'test': ('test', {}, None),
    'main': ('main', {'access_level': 'standard'}, None),
    'output_version': ('test', {'output_version': 'My Version.1'}, None),
    'duplicate_cohort_ids': ('test', {'input_cohorts': ['COH1', 'COH2', 'COH1']}, None),
    'overlapping_cohorts': ('test', {}, _overlap),
    'inactive_cohort': ('test', {}, _inactive),
    'no_pedigree': ('test', {'read_pedigree': False}, None),
}


def run_config(ns: str, overrides: dict[str, Any]) -> dict[str, Any]:
    access_level = 'test' if ns == 'test' else 'standard'
    return {
        'workflow': {'access_level': access_level, 'input_cohorts': ['COH2', 'COH1'], 'sequencing_type': 'genome'}
        | overrides
    }


def old_config(run: dict[str, Any], ref_ped: Path) -> dict[str, Any]:
    """What analysis-runner and the config template would give cpg-flow: the run config plus storage etc."""
    config = copy.deepcopy(run)
    access_level = config['workflow']['access_level']
    config['workflow']['dataset'] = 'analysis-ds'
    config['workflow']['gatk_sv_commit'] = 'unused'
    config['storage'] = {'default': cfg.storage_section('analysis-ds', access_level)}
    for ds in DATASETS:
        config['storage'][ds] = cfg.storage_section(ds, access_level)
    config['references'] = {'ped_file': str(ref_ped), 'broad': {'ref_fasta': 'gs://ref/fasta'}}
    return config


def new_side(run: dict[str, Any], data: dict[str, Any]) -> dict[str, Any]:
    config = cfg.merge_layers(run, dataset='analysis-ds', billing_project='bp', driver_image='img')
    try:
        m = build_manifest(
            ManifestRequest.from_config(config, find_previous_spicy_vcf=True), Metamist(FakeMetamist(data).query)
        )
    except Exception as e:  # noqa: BLE001 (a refusal is compared, not raised)
        return {'error': f'{type(e).__name__}: {e}'}
    try:
        check_for_cohort_overlaps(m)
        overlaps = None
    except ValueError as e:
        overlaps = str(e)
    peds = {c.id: combined_ped(m.cohort_sequencing_groups(c.id), REF_PED, c.id) for c in m.cohorts}
    peds['multicohort'] = combined_ped(m.sequencing_groups, REF_PED, m.name)
    return {
        'name': m.name,
        'analysis_dataset': m.analysis_dataset,
        'output_version': m.output_version,
        'cohorts': [
            {'id': c.id, 'name': c.name, 'dataset': c.dataset, 'sequencing_groups': list(c.sequencing_group_ids)}
            for c in m.cohorts
        ],
        'datasets': m.dataset_names,
        'active_sg_ids': {ds: list(ids) for ds, ids in m.active_sg_ids.items()},
        'sequencing_groups': [
            {
                'id': sg.id,
                'dataset': sg.dataset,
                'cram': sg.cram,
                'participant_id': sg.participant_id,
                'external_id': sg.external_id,
                'meta': sg.meta,
                'ped': sg.ped_fields(),
                'job_attrs': sg.job_attrs(),
            }
            for sg in m.sequencing_groups
        ],
        'previous_spicy_vcf': m.previous_spicy_vcf,
        'peds': peds,
        'overlaps': overlaps,
    }


def diff(old: Any, new: Any, path: str = '') -> list[str]:
    if isinstance(old, dict) and isinstance(new, dict):
        out = []
        for key in sorted(set(old) | set(new)):
            if key not in old or key not in new:
                out.append(f'{path}/{key}: only on the {"new" if key not in old else "old"} side')
            else:
                out += diff(old[key], new[key], f'{path}/{key}')
        return out
    if isinstance(old, list) and isinstance(new, list) and len(old) == len(new):
        return [d for i, (o, n) in enumerate(zip(old, new, strict=True)) for d in diff(o, n, f'{path}[{i}]')]
    return [] if old == new else [f'{path}: old {old!r} != new {new!r}']


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument('--old-python', default=str(OLD_REPO / '.venv' / 'bin' / 'python'))
    parser.add_argument('--keep', help='keep the per-variant inputs and outputs here')
    args = parser.parse_args()

    root = Path(args.keep or tempfile.mkdtemp(prefix='p1-'))
    failed = []
    for name, (ns, overrides, edit) in VARIANTS.items():
        work = root / name
        work.mkdir(parents=True, exist_ok=True)
        data = fixture(ns)
        if edit:
            data = edit(data)
        run = run_config(ns, overrides)
        (work / 'fixture.json').write_text(json.dumps(data))
        (work / 'ref.ped').write_text(REF_PED)
        (work / 'old_config.toml').write_text(toml.dumps(old_config(run, work / 'ref.ped')))

        proc = subprocess.run(
            [
                args.old_python,
                str(REPO / 'parity' / 'p1_old_side.py'),
                '--config',
                str(work / 'old_config.toml'),
                '--fixture',
                str(work / 'fixture.json'),
                '--out',
                str(work / 'old.json'),
            ],
            capture_output=True,
            text=True,
            check=False,
        )
        if proc.returncode != 0:
            print(f'[{name}] old side failed:\n{proc.stderr[-1500:]}')
            failed.append(name)
            continue
        old = json.loads((work / 'old.json').read_text())
        new = new_side(run, data)
        (work / 'new.json').write_text(json.dumps(new, indent=2, sort_keys=True))

        if differences := diff(old, new):
            failed.append(name)
            print(f'[{name}] DIFFERENT:')
            for line in differences:
                print(f'  {line}')
        else:
            summary = old.get('error') or f'{len(old["sequencing_groups"])} SGs, {len(old["cohorts"])} cohorts'
            print(f'[{name}] identical ({summary})')

    print(f'\n{len(VARIANTS) - len(failed)} of {len(VARIANTS)} variants identical. Outputs in {root}')
    return 1 if failed else 0


if __name__ == '__main__':
    sys.exit(main())
