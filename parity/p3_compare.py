#!/usr/bin/env python3
"""
P3 comparison (NEXTFLOW_PORT_PLAN.md §10): old-side capture vs new-side `--dump-inputs`, per SG.

    python3 parity/p3_compare.py old.json new.json

Exits 1 on any difference. Compared exactly: the prefixed Cromwell inputs, the workflow options (including
google_labels, so ar-guid too), the outputs to collect and where they go, the workflow file, the Cromwell dataset
and access level, output_prefix, copy_outputs, the polling bounds before jitter and the time limit.

Not compared: google_compute_service_account, which is the client_email of each side's fake secret. The options
fixture (test/fixtures/cromwell_options_parity.json) covers how it is derived. The old side's bundle
source (repo, cwd, libs) is checked against what `nf-gatk-sv prepare-wdl` bundles.
"""

import json
import sys
from pathlib import Path
from typing import Any

COMPARED = (
    'inputs',
    'options',
    'collect',
    'workflow',
    'dataset',
    'access_level',
    'output_prefix',
    'copy_outputs',
    'poll',
    'time_limit_seconds',
)
NOT_COMPARED_OPTIONS = ('google_compute_service_account',)
# what `nf-gatk-sv prepare-wdl` bundles (gatk_sv_nf.stages._cromwell_base)
EXPECTED_SOURCE = {'repo': 'gatk-sv', 'cwd': 'wdl', 'libs': ['.']}


def canonical(value: Any) -> str:
    return json.dumps(value, sort_keys=True)


def diff_dicts(old: dict, new: dict, where: str) -> list[str]:
    out = []
    for key in sorted(set(old) | set(new)):
        if key not in new:
            out.append(f'{where}.{key}: only in old ({canonical(old[key])})')
        elif key not in old:
            out.append(f'{where}.{key}: only in new ({canonical(new[key])})')
        elif canonical(old[key]) != canonical(new[key]):
            out.append(f'{where}.{key}: old {canonical(old[key])} != new {canonical(new[key])}')
    return out


def compare_sg(old: dict[str, Any], new: dict[str, Any]) -> list[str]:
    problems = []
    for field in COMPARED:
        o, n = old.get(field), new.get(field)
        if field == 'options' and isinstance(o, dict) and isinstance(n, dict):
            o = {k: v for k, v in o.items() if k not in NOT_COMPARED_OPTIONS}
            n = {k: v for k, v in n.items() if k not in NOT_COMPARED_OPTIONS}
        if isinstance(o, dict) and isinstance(n, dict):
            problems += diff_dicts(o, n, field)
        elif canonical(o) != canonical(n):
            problems.append(f'{field}: old {canonical(o)} != new {canonical(n)}')

    source = old.get('source', {})
    for key, expected in EXPECTED_SOURCE.items():
        if source.get(key) != expected:
            problems.append(f'source.{key}: old {source.get(key)!r}, but prepare-wdl bundles {expected!r}')
    if set(old.get('outputs_to_collect', {})) != set(old.get('collect') or {}):
        problems.append('old side: outputs_to_collect and expected_out_dict have different keys')
    return problems


def main(argv: list[str]) -> int:
    if len(argv) != 2:
        print(__doc__, file=sys.stderr)
        return 2
    old = json.loads(Path(argv[0]).read_text())
    new = json.loads(Path(argv[1]).read_text())

    failed = False
    if set(old) != set(new):
        failed = True
        print(f'SG sets differ: only old {sorted(set(old) - set(new))}, only new {sorted(set(new) - set(old))}')
    for sg_id in sorted(set(old) & set(new)):
        problems = compare_sg(old[sg_id], new[sg_id])
        if problems:
            failed = True
            print(f'{sg_id}: {len(problems)} difference(s)')
            for p in problems:
                print(f'  {p}')
        else:
            print(f'{sg_id}: identical ({len(old[sg_id]["inputs"])} inputs, {len(old[sg_id]["collect"])} outputs)')
    return 1 if failed else 0


if __name__ == '__main__':
    sys.exit(main(sys.argv[1:]))
