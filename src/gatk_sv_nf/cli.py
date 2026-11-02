"""
`nf-gatk-sv` CLI (plan §3, §5.2).

DRAFT: `plan` (the `PLAN` process, plan §5.4), `prepare-wdl` (the `PREPARE_WDL` process) and
`stage GatherSampleEvidence --chunk ...` (the `GSE_CHUNK` process, plan §5.6). The per-target `stage` form comes later.

    nf-gatk-sv plan --entry sample_evidence --config gs://.../run.toml --dataset <ds> --out plan.json
    nf-gatk-sv plan ... --dry-run          # writes nothing to GCS; prints plan.json
    nf-gatk-sv plan ... --print-config     # prints the resolved config; writes nothing
    nf-gatk-sv prepare-wdl --plan gs://.../plans/<hash>
    nf-gatk-sv stage GatherSampleEvidence --plan gs://.../plans/<hash> \
        --chunk gs://.../plans/<hash>/gse_chunks/chunk_0.json --ar-guid <arGuid> --bundle <uri> --out-dir outputs

Exit codes are cpg-cromwell's (0, 1, 2, 75, 124, 143/130), combined over the chunk.
"""

import argparse
import json
import logging
import sys
from pathlib import Path

from cpg_cromwell import EXIT_OK, EXIT_USAGE, CromwellCliError, RunContext, SpecError, build_bundle, render
from cpg_cromwell.storage import join
from gatk_sv_nf import plan as planning
from gatk_sv_nf.config import ConfigError
from gatk_sv_nf.manifest import load_sequencing_groups
from gatk_sv_nf.metamist_client import Metamist, MetamistError
from gatk_sv_nf.stages import gather_sample_evidence as gse
from gatk_sv_nf.stages._cromwell_base import GATK_SV_REPO, GATK_SV_WDL_DIR, bundle_root, default_reattach_root

logger = logging.getLogger('gatk_sv_nf')

OFFLINE_SECRET = json.dumps({'client_email': '<offline: secret not read>'})


def _read(uri: str, ctx: RunContext) -> str:
    assert ctx.storage is not None
    text = ctx.storage.read_text(uri)
    if text is None:
        raise SpecError(f'not found: {uri}')
    return text


def _use_plan_config(plan_dir: str) -> None:
    from cpg_utils.config import set_config_paths  # noqa: PLC0415

    set_config_paths([join(plan_dir, 'config.toml')])


def cmd_plan(args: argparse.Namespace, ctx: RunContext, metamist: Metamist | None = None) -> int:
    """Resolve the config, build the manifest, write the plan dir; write plan.json to --out (plan §5.4)."""
    assert ctx.storage is not None
    dry_run = args.dry_run or args.print_config
    try:
        plan, config = planning.make_plan(
            entry=args.entry,
            run_config=planning.read_run_config(args.config, ctx.storage),
            dataset=args.dataset,
            billing_project=planning.resolve_billing_project(args.billing_project, dry_run=dry_run),
            driver_image=args.driver_image or ('unset' if dry_run else ''),
            storage=ctx.storage,
            metamist=metamist or Metamist(),
            gse_chunk_size=args.gse_chunk_size,
        )
    except (ConfigError, planning.PlanError, MetamistError) as e:
        logger.error('%s', e)
        return EXIT_USAGE

    if args.print_config:
        print(plan.config_text, end='')
        return EXIT_OK
    plan_json = json.dumps(plan.plan_json(config), indent=2) + '\n'
    if args.dry_run:
        print(plan_json, end='')
    else:
        planning.write_plan(plan, ctx.storage)
    if args.out:
        Path(args.out).write_text(plan_json)
    return EXIT_OK


def cmd_prepare_wdl(args: argparse.Namespace, ctx: RunContext) -> int:
    """
    Bundle gatk-sv@workflow.gatk_sv_commit once per run (old `prepare_git_job`), and print its URI.
    Reuses an existing bundle. For non-test access levels the commit must be on main.
    """
    from cpg_utils.config import config_retrieve  # noqa: PLC0415

    _use_plan_config(args.plan)
    uri = build_bundle(
        GATK_SV_REPO,
        config_retrieve(['workflow', 'gatk_sv_commit']),
        cwd=GATK_SV_WDL_DIR,
        libs=['.'],
        dest=bundle_root(),
        require_main=config_retrieve(['workflow', 'access_level']) != 'test',
        storage=ctx.storage,
    )
    print(uri)
    return EXIT_OK


def cmd_gather_sample_evidence(args: argparse.Namespace, ctx: RunContext) -> int:
    _use_plan_config(args.plan)
    all_sgs = load_sequencing_groups(_read(join(args.plan, 'manifest.json'), ctx))
    sg_ids = json.loads(_read(args.chunk, ctx))
    missing = [sg_id for sg_id in sg_ids if sg_id not in all_sgs]
    if missing:
        raise SpecError(f'chunk lists sequencing groups not in the manifest: {missing}')
    sgs = [all_sgs[sg_id] for sg_id in sg_ids]

    if args.dump_inputs:
        assert ctx.storage is not None
        dump = {}
        for sg in sgs:
            spec = gse.build_sg_spec(sg, ar_guid=args.ar_guid, exists=ctx.storage.exists, bundle=args.bundle)
            prefixed, options = render(spec, context=ctx, secret_json=OFFLINE_SECRET)
            # everything P3 compares with the old submission (parity/p3_compare.py)
            dump[sg.id] = {
                'inputs': prefixed,
                'options': options,
                'collect': spec.collect,
                'workflow': spec.source.workflow,
                'dataset': spec.dataset,
                'access_level': spec.access_level,
                'output_prefix': spec.output_prefix,
                'copy_outputs': spec.copy_outputs,
                # bounds before jitter, as `utils.create_polling_intervals` gives them
                'poll': {'min': spec.poll.min_seconds, 'max': spec.poll.max_seconds},
                'time_limit_seconds': spec.poll.time_limit_seconds,
            }
        Path(args.dump_inputs).write_text(json.dumps(dump, indent=2, sort_keys=True))
        return 0

    report = gse.run_chunk(
        sgs,
        ar_guid=args.ar_guid,
        out_dir=Path(args.out_dir),
        reattach_root=args.reattach_root or default_reattach_root(),
        results_dir=args.results_dir,
        bundle=args.bundle,
        registrar=None if args.no_register else gse.register_outputs,
        submit_interval=args.submit_interval,
        max_in_flight=args.max_in_flight,
        context=ctx,
    )
    return report.exit_code


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog='nf-gatk-sv')
    parser.add_argument('-v', '--verbose', action='store_true')
    sub = parser.add_subparsers(dest='command', required=True)

    p = sub.add_parser('plan', help='resolve the config, query metamist and write the plan dir (PLAN)')
    p.add_argument('--entry', required=True, choices=['sample_evidence', 'evidence_qc', 'multi_sample'])
    p.add_argument('--config', required=True, help="the user's run TOML (local or gs://)")
    p.add_argument('--dataset', required=True, help='the analysis dataset: becomes workflow.dataset')
    p.add_argument('--billing-project', help='default: the project of this VM, from the metadata server')
    p.add_argument('--driver-image', help='becomes workflow.driver_image (required unless --dry-run)')
    p.add_argument('--gse-chunk-size', type=int, default=planning.DEFAULT_GSE_CHUNK_SIZE)
    p.add_argument('--out', help='write plan.json here')
    p.add_argument('--dry-run', action='store_true', help='write nothing to GCS; print plan.json')
    p.add_argument('--print-config', action='store_true', help='print the resolved config; write nothing')
    p.set_defaults(func=cmd_plan)

    w = sub.add_parser('prepare-wdl', help='bundle the gatk-sv WDL for workflow.gatk_sv_commit; prints the URI')
    w.add_argument('--plan', required=True, help='plan dir holding config.toml')
    w.set_defaults(func=cmd_prepare_wdl)

    stage = sub.add_parser('stage', help='run one stage')
    stages = stage.add_subparsers(dest='stage', required=True)

    g = stages.add_parser(gse.STAGE_NAME, help='one chunk of sequencing groups (GSE_CHUNK)')
    g.add_argument('--plan', required=True, help='plan dir holding config.toml and manifest.json')
    g.add_argument('--chunk', required=True, help='JSON list of SG IDs (local or gs://)')
    g.add_argument('--ar-guid', required=True, help='params.arGuid; becomes the ar-guid billing label')
    g.add_argument('--out-dir', default='outputs', help='writes <sg_id>.json per finished SG')
    g.add_argument('--bundle', help='gatk-sv bundle URI (default: the PREPARE_WDL location for gatk_sv_commit)')
    g.add_argument('--reattach-root', help='default gs://<tmp>/gatk_sv_nf/cromwell_ids')
    g.add_argument('--results-dir', help='also write one cpg-cromwell result JSON per workflow here')
    g.add_argument('--submit-interval', type=float, default=1.0)
    g.add_argument('--max-in-flight', type=int)
    g.add_argument('--no-register', action='store_true', help="don't create metamist analyses (crossover runs)")
    g.add_argument('--dump-inputs', metavar='FILE', help='write the rendered Cromwell inputs/options; submit nothing')
    g.set_defaults(func=cmd_gather_sample_evidence)
    return parser


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    logging.basicConfig(
        level=logging.DEBUG if args.verbose else logging.INFO,
        format='%(asctime)s %(levelname)s %(name)s: %(message)s',
        stream=sys.stderr,
    )
    ctx = RunContext().resolved()
    try:
        return args.func(args, ctx)
    except CromwellCliError as e:
        logger.error('%s', e)
        return e.exit_code
    except (KeyError, ValueError) as e:  # malformed manifest/chunk/config
        logger.error('%s: %s', type(e).__name__, e)
        return EXIT_USAGE


if __name__ == '__main__':
    sys.exit(main())
