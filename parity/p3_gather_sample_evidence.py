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
import sys
from pathlib import Path
from types import SimpleNamespace
from typing import Any

from p3_old_common import capture, guid_config_paths, load_config, make_sg, patch_old_stack, read_text

STAGE = 'GatherSampleEvidence'


def capture_sg(sg_data: dict[str, Any], config_paths: list[str], existing: set[str] | None) -> dict[str, Any]:
    load_config(config_paths)
    recorded = patch_old_stack()

    from cpg_flow_gatk_sv.jobs import GatherSampleEvidence as gse_job
    from cpg_flow_gatk_sv.singlesample_workflow import GatherSampleEvidence
    from cpg_utils import to_path

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

    sg = make_sg(sg_data, {})
    # the stage class is behind cpg-flow's @stage decorator; expected_outputs only reads self.name
    expected = GatherSampleEvidence.__wrapped__.expected_outputs(SimpleNamespace(name=STAGE), sg)
    gse_job.create_gather_sample_evidence_jobs(sg=sg, expected_outputs=expected)
    # the job pops the reused Manta VCF out of `expected` in place, so this is what add_gatk_sv_jobs collected
    return capture(recorded, expected)


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument('--plan', required=True, help='plan dir holding config.toml and manifest.json')
    parser.add_argument('--chunk', required=True, help='JSON list of SG IDs')
    parser.add_argument('--ar-guid', required=True)
    parser.add_argument('--existing', help='JSON list of gs:// paths to treat as existing; default: check GCS')
    parser.add_argument('--out', required=True)
    args = parser.parse_args()

    plan = args.plan.rstrip('/')
    config_paths = guid_config_paths(plan, args.ar_guid)
    manifest = json.loads(read_text(f'{plan}/manifest.json'))
    sgs = {sg['id']: sg for sg in manifest['sequencing_groups']}
    existing = set(json.loads(Path(args.existing).read_text())) if args.existing else None

    dump = {sg_id: capture_sg(sgs[sg_id], config_paths, existing) for sg_id in json.loads(read_text(args.chunk))}
    Path(args.out).write_text(json.dumps(dump, indent=2, sort_keys=True))
    print(f'wrote {args.out} ({len(dump)} SGs)', file=sys.stderr)
    return 0


if __name__ == '__main__':
    sys.exit(main())
