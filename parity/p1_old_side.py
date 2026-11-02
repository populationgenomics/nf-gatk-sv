#!/usr/bin/env python3
"""
P1, old side: cpg-flow's view of a multicohort, built from a canned metamist (test/fake_metamist.py).

Runs in cpg-flow-gatk-sv's env (driven by parity/p1_offline.py). Monkeypatches only; the old repo isn't edited.

    python p1_old_side.py --config C.toml --fixture F.json --out OUT.json
"""

import argparse
import json
import os
import sys
import tempfile
import traceback
from pathlib import Path

HERE = Path(__file__).resolve().parent
sys.path.insert(0, str(HERE.parent / 'test'))


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument('--config', required=True)
    parser.add_argument('--fixture', required=True)
    parser.add_argument('--out', required=True)
    args = parser.parse_args()

    os.environ['CPG_CONFIG_PATH'] = args.config  # before cpg_flow_gatk_sv.utils reads it at import

    import cpg_flow.inputs
    import cpg_flow.metamist
    from cpg_flow.utils import slugify
    from cpg_flow_gatk_sv import utils
    from cpg_utils.config import config_retrieve
    from fake_metamist import FakeMetamist, _text

    fixture = json.loads(Path(args.fixture).read_text())
    fake = FakeMetamist(fixture)

    def old_query(document, variables=None, **kwargs):
        # cpg-flow-gatk-sv's get_active_sg_ids queries the unsuffixed project name; at access level test that
        # project isn't the one cpg-flow read the SGs from. Answer from the -test project, so the rest compares
        v = dict(variables or {})
        if 'activeOnly' in _text(document):
            proj = v['metamist_proj']
            if proj not in fixture['projects'] and f'{proj}-test' in fixture['projects']:
                v['metamist_proj'] = f'{proj}-test'
        return fake.query(document, v, **kwargs)

    cpg_flow.metamist.query = old_query
    utils.query = old_query
    cpg_flow.metamist.AnalysisApi = lambda: None  # Metamist() builds one; nothing here calls it
    cpg_flow.inputs.exists = lambda path: str(path).endswith('.cram')  # registered CRAMs exist, nothing else

    out: dict = {}
    try:
        mc = cpg_flow.inputs.get_multicohort()
    except Exception as e:  # noqa: BLE001 (compared with the new side's refusal)
        out['error'] = f'{type(e).__name__}: {e}'
        traceback.print_exc()
        Path(args.out).write_text(json.dumps(out, indent=2, sort_keys=True))
        return

    if output_version := config_retrieve(['workflow', 'output_version'], None):
        output_version = slugify(output_version)
    else:
        output_version = mc.get_sg_hash()  # Workflow.output_version, without building a Workflow

    out['name'] = mc.name
    out['analysis_dataset'] = mc.analysis_dataset.name
    out['output_version'] = output_version
    out['cohorts'] = [
        {'id': c.id, 'name': c.name, 'dataset': c.dataset.name, 'sequencing_groups': c.get_sequencing_group_ids()}
        for c in mc.get_cohorts()
    ]
    out['datasets'] = [d.name for d in mc.get_datasets()]
    out['active_sg_ids'] = {d.name: list(utils.get_active_sg_ids(d)) for d in mc.get_datasets()}
    out['sequencing_groups'] = [
        {
            'id': sg.id,
            'dataset': sg.dataset.name,
            'cram': str(sg.cram) if sg.cram else None,
            'participant_id': sg._participant_id,
            'external_id': sg._external_id,
            'meta': sg.meta,
            'ped': list(sg.pedigree.get_ped_dict().values()),
            'job_attrs': sg.get_job_attrs(),
        }
        for sg in mc.get_sequencing_groups()
    ]
    out['previous_spicy_vcf'] = utils.query_for_spicy_vcf(mc.analysis_dataset.name)

    with tempfile.TemporaryDirectory() as tmp:
        peds = {}
        for target in [*mc.get_cohorts(), mc]:
            path = Path(tmp) / 'ped.ped'
            utils.make_combined_ped(target, path)
            peds['multicohort' if target is mc else target.id] = path.read_text()
        out['peds'] = peds
    try:
        utils.check_for_cohort_overlaps(mc)
        out['overlaps'] = None
    except ValueError as e:
        out['overlaps'] = str(e)

    Path(args.out).write_text(json.dumps(out, indent=2, sort_keys=True))


if __name__ == '__main__':
    main()
