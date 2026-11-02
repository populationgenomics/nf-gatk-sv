"""`cpg-cromwell` command line (design §6). Thin: every command calls the Python API."""

import argparse
import json
import logging
import os
import sys
from collections.abc import Sequence

from cpg_cromwell.api import CromwellRequestError
from cpg_cromwell.bundle import build_bundle
from cpg_cromwell.errors import EXIT_FAILED, EXIT_OK, EXIT_USAGE, CromwellCliError, SpecError, combine_exit_codes
from cpg_cromwell.run import RunContext, render, run, run_many
from cpg_cromwell.spec import WorkflowSpec, read_specs_jsonl
from cpg_cromwell.storage import Storage

logger = logging.getLogger('cpg_cromwell')


def _read(uri: str, storage: Storage) -> str:
    if uri == '-':
        return sys.stdin.read()
    text = storage.read_text(uri)
    if text is None:
        raise SpecError(f'{uri} not found')
    return text


def _config_access_level() -> str | None:
    if not os.getenv('CPG_CONFIG_PATH'):
        return None
    try:
        from cpg_utils.config import config_retrieve  # noqa: PLC0415

        return config_retrieve(['workflow', 'access_level'], default=None)
    except Exception:  # noqa: BLE001
        return None


def cmd_bundle(args: argparse.Namespace, ctx: RunContext) -> int:
    access_level = args.access_level or _config_access_level()
    if access_level is None:
        logger.info('no --access-level and none in config: requiring the commit to be on main')
    uri = build_bundle(
        args.repo,
        args.commit,
        cwd=args.cwd,
        libs=args.lib or ['.'],
        organisation=args.org,
        dest=args.dest,
        require_main=access_level != 'test',
        storage=ctx.storage,
    )
    print(uri)
    return EXIT_OK


def cmd_render(args: argparse.Namespace, ctx: RunContext) -> int:
    spec = WorkflowSpec.from_json(_read(args.spec, ctx.storage))  # type: ignore[arg-type]
    secret = None
    if args.offline:
        secret = json.dumps({'client_email': '<offline: secret not read>'})
    inputs, options = render(spec, context=ctx, secret_json=secret)
    print(json.dumps({'inputs': inputs, 'options': options}, indent=2))
    return EXIT_OK


def cmd_run(args: argparse.Namespace, ctx: RunContext) -> int:
    spec = WorkflowSpec.from_json(_read(args.spec, ctx.storage))  # type: ignore[arg-type]
    result = run(spec, reattach_root=args.reattach_root, abort_on_signal=not args.no_abort_on_exit, context=ctx)
    if args.result_json:
        ctx.storage.write(args.result_json, result.to_json(indent=2))  # type: ignore[union-attr]
    if result.ok and args.outputs_json:
        # written last: if it exists, collection finished
        ctx.storage.write(args.outputs_json, json.dumps(result.outputs, indent=2))  # type: ignore[union-attr]
    return result.exit_code


def cmd_run_many(args: argparse.Namespace, ctx: RunContext) -> int:
    specs = read_specs_jsonl(_read(args.specs, ctx.storage))  # type: ignore[arg-type]
    results = run_many(
        specs,
        reattach_root=args.reattach_root,
        submit_interval=args.submit_interval,
        max_in_flight=args.max_in_flight,
        results_dir=args.results_dir,
        abort_on_signal=not args.no_abort_on_exit,
        context=ctx,
    )
    return combine_exit_codes([r.exit_code for r in results])


def cmd_status(args: argparse.Namespace, ctx: RunContext) -> int:
    status = ctx.api.status(args.workflow_id)  # type: ignore[union-attr]
    print(json.dumps({'id': args.workflow_id, 'status': status}) if args.json else status)
    return EXIT_OK


def cmd_metadata(args: argparse.Namespace, ctx: RunContext) -> int:
    metadata = ctx.api.metadata(args.workflow_id, expand_subworkflows=args.expand_subworkflows)  # type: ignore[union-attr]
    if args.display:
        from cpg_utils.cromwell_model import WorkflowMetadataModel  # noqa: PLC0415

        metadata.setdefault('calls', {})
        try:
            model = WorkflowMetadataModel.parse(metadata)
            print(model.display(expand_completed=args.expand_completed, monochrome=not sys.stdout.isatty()))
            return EXIT_OK
        except (ValueError, KeyError, TypeError) as e:  # e.g. cpg_utils has no 'Aborting' ExecutionStatus
            logger.warning('cannot display this metadata (%s); printing JSON instead', e)
    print(json.dumps(metadata, indent=2))
    return EXIT_OK


def cmd_outputs(args: argparse.Namespace, ctx: RunContext) -> int:
    print(json.dumps(ctx.api.outputs(args.workflow_id), indent=2))  # type: ignore[union-attr]
    return EXIT_OK


def cmd_abort(args: argparse.Namespace, ctx: RunContext) -> int:
    code = EXIT_OK
    for workflow_id in args.workflow_ids:
        try:
            print(f'{workflow_id}\t{ctx.api.abort(workflow_id)}')  # type: ignore[union-attr]
        except CromwellRequestError as e:
            print(f'{workflow_id}\terror: {e}')
            code = EXIT_FAILED
    return code


def build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(prog='cpg-cromwell', description='Hail-free client for the CPG Cromwell server')
    p.add_argument('-v', '--verbose', action='store_true', help='debug logging')
    sub = p.add_subparsers(dest='command', required=True)

    b = sub.add_parser('bundle', help='clone repo@commit, check it is on main, upload a WDL bundle')
    b.add_argument('--repo', required=True)
    b.add_argument('--commit', required=True, help='full 40-character SHA')
    b.add_argument('--cwd', default='', help='directory inside the repo holding the WDL (e.g. wdl)')
    b.add_argument(
        '--lib', action='append', help='path (relative to --cwd) to zip into deps.zip; repeatable; default "."'
    )
    b.add_argument('--org', default='populationgenomics')
    b.add_argument('--dest', required=True, help='bundle root, e.g. gs://cpg-<dataset>-<ns>-tmp/.../bundles')
    b.add_argument(
        '--access-level',
        help='only "test" may bundle commits not on main (default: config workflow.access_level, else strict)',
    )
    b.set_defaults(func=cmd_bundle)

    r = sub.add_parser('render', help='print the final inputs and options (secret redacted); submits nothing')
    r.add_argument('--spec', required=True, help='spec JSON file (local, gs:// or - for stdin)')
    r.add_argument(
        '--offline', action='store_true', help="don't read the Cromwell secret (service account shown as a placeholder)"
    )
    r.set_defaults(func=cmd_render)

    def common_run(sp: argparse.ArgumentParser) -> None:
        sp.add_argument('--reattach-root', help='where re-attach records live (gs://...); omit to always submit')
        sp.add_argument('--no-abort-on-exit', action='store_true', help='on SIGTERM/SIGINT, leave workflows running')

    rr = sub.add_parser('run', help='submit (or re-attach), watch and collect one workflow')
    rr.add_argument('--spec', required=True, help='spec JSON file (local, gs:// or - for stdin)')
    rr.add_argument('--outputs-json', help='write {key: destination or value} here on success, last')
    rr.add_argument('--result-json', help='write the WorkflowResult here, success or not')
    common_run(rr)
    rr.set_defaults(func=cmd_run)

    rm = sub.add_parser('run-many', help='run many workflows from a JSONL file, one spec per line')
    rm.add_argument('--specs', required=True)
    rm.add_argument(
        '--results-dir', required=True, help='one <name>.json result per workflow, written as each finishes'
    )
    rm.add_argument('--submit-interval', type=float, default=1.0, help='seconds between submissions')
    rm.add_argument('--max-in-flight', type=int, help='cap on workflows running at once')
    common_run(rm)
    rm.set_defaults(func=cmd_run_many)

    s = sub.add_parser('status', help='print a workflow status')
    s.add_argument('workflow_id')
    s.add_argument('--json', action='store_true')
    s.set_defaults(func=cmd_status)

    m = sub.add_parser('metadata', help='print workflow metadata')
    m.add_argument('workflow_id')
    m.add_argument('--display', action='store_true', help='human-readable summary (WorkflowMetadataModel.display)')
    m.add_argument('--expand-completed', action='store_true')
    m.add_argument('--expand-subworkflows', action='store_true')
    m.set_defaults(func=cmd_metadata)

    o = sub.add_parser('outputs', help='print workflow outputs')
    o.add_argument('workflow_id')
    o.set_defaults(func=cmd_outputs)

    a = sub.add_parser('abort', help='abort workflows')
    a.add_argument('workflow_ids', nargs='+')
    a.set_defaults(func=cmd_abort)
    return p


def main(argv: Sequence[str] | None = None, context: RunContext | None = None) -> int:
    args = build_parser().parse_args(argv)
    logging.basicConfig(
        level=logging.DEBUG if args.verbose else logging.INFO,
        format='%(asctime)s %(levelname)s %(name)s: %(message)s',
        stream=sys.stderr,
    )
    if not args.verbose:
        for noisy in ('urllib3', 'google', 'google.auth'):
            logging.getLogger(noisy).setLevel(logging.WARNING)
    try:
        ctx = (context or RunContext()).resolved()
        return args.func(args, ctx)
    except CromwellCliError as e:
        logger.error('%s', e)
        return e.exit_code
    except CromwellRequestError as e:
        logger.error('%s', e)
        return EXIT_USAGE if e.status_code == 400 else EXIT_FAILED


if __name__ == '__main__':
    sys.exit(main())
