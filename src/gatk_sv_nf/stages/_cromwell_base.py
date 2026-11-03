"""
Builds a `gatk_sv_nf.cromwell.WorkflowSpec` for a GATK-SV stage (plan §5.1), and runs one per target.

Port of `cpg_flow_gatk_sv.utils.add_gatk_sv_jobs`, `create_polling_intervals` and `CromwellJobSizes`. Submitting,
watching, re-attaching and copying outputs are `cpg_utils.cromwell_client`'s, through `gatk_sv_nf.cromwell`.
"""

import enum
import json
import logging
from collections.abc import Callable, Mapping
from pathlib import Path
from typing import Any

from cpg_utils.config import config_retrieve

from gatk_sv_nf.cromwell import RunContext, WorkflowSpec, render, run
from gatk_sv_nf.errors import EXIT_FAILED, EXIT_OK
from gatk_sv_nf.paths import analysis_dataset_tmp
from gatk_sv_nf.wdl_bundle import bundle_uri

logger = logging.getLogger(__name__)

GATK_SV_REPO = 'gatk-sv'


class CromwellJobSize(enum.Enum):
    SMALL = 'small'
    MEDIUM = 'medium'
    LARGE = 'large'


DEFAULT_POLLING_INTERVALS: dict[CromwellJobSize, dict[str, int]] = {
    CromwellJobSize.SMALL: {'min': 30, 'max': 140},
    CromwellJobSize.MEDIUM: {'min': 40, 'max': 400},
    CromwellJobSize.LARGE: {'min': 200, 'max': 2000},
}


def poll_bounds(size: CromwellJobSize) -> dict[str, int]:
    """
    Per-size polling bounds before jitter, overridable from `[cromwell_polling_intervals.<size>]`. Jitter
    (`randint(min, 2*min)` / `randint(max, 2*max)`, once per workflow) is added by `gatk_sv_nf.cromwell.polling`.
    """
    bounds = dict(DEFAULT_POLLING_INTERVALS[size])
    if override := config_retrieve(['cromwell_polling_intervals', size.value], None):
        bounds.update(override)
    return bounds


def apply_resource_overrides(wfl_name: str, inputs: dict[str, Any]) -> dict[str, Any]:
    """`input_dict |= [resource_overrides.<wfl_name>]`, before the keys are prefixed."""
    if override := config_retrieve(['resource_overrides', wfl_name], None):
        return inputs | dict(override)
    return inputs


GATK_SV_WDL_DIR = 'wdl'  # `cwd` of the old prepare_git_job; the whole of it is zipped (libs=['.'])


def bundle_root() -> str:
    """Root of the run's WDL bundles: `gs://<tmp of workflow.dataset>/gatk_sv_nf/bundles`."""
    return analysis_dataset_tmp('gatk_sv_nf/bundles')


def default_bundle() -> str:
    """Where `PREPARE_WDL` (`nf-gatk-sv prepare-wdl`) puts the gatk-sv WDL for `workflow.gatk_sv_commit`."""
    return bundle_uri(bundle_root(), GATK_SV_REPO, config_retrieve(['workflow', 'gatk_sv_commit']))


def build_spec(
    *,
    wfl_name: str,
    target_dataset: str,
    inputs: Mapping[str, Any],
    collect: Mapping[str, str | list[str]],
    labels: Mapping[str, str],
    ar_guid: str,
    size: CromwellJobSize = CromwellJobSize.MEDIUM,
    sequencing_group_id: str | None = None,
    bundle: str | None = None,
) -> WorkflowSpec:
    """
    One Cromwell submission. `inputs` are unprefixed and get the resource overrides merged in here;
    `collect` is the stage's expected outputs, whose keys must equal the WDL output names.

    The Cromwell `dataset` (secret, log and tmp buckets) is always `workflow.dataset`, never the
    target's; the target's dataset only appears in `output_prefix`.
    """
    output_prefix = f'gatk_sv/output/{wfl_name}/{target_dataset}'
    if sequencing_group_id:
        output_prefix = f'{output_prefix}/{sequencing_group_id}'
    bounds = poll_bounds(size)
    return WorkflowSpec(
        name=output_prefix.removeprefix('gatk_sv/output/'),
        bundle=bundle or default_bundle(),
        workflow=f'{wfl_name}.wdl',
        input_prefix=wfl_name,
        inputs=apply_resource_overrides(wfl_name, dict(inputs)),
        dataset=config_retrieve(['workflow', 'dataset']),
        access_level=config_retrieve(['workflow', 'access_level']),
        output_prefix=output_prefix,
        ar_guid=ar_guid,
        labels=dict(labels),
        copy_outputs=config_retrieve(['workflow', 'copy_outputs'], False),
        collect=dict(collect),
        poll_min_seconds=bounds['min'],
        poll_max_seconds=bounds['max'],
        time_limit_seconds=config_retrieve(['workflow', 'time_limit_seconds'], None),
    )


# ---- one workflow per target (every Cromwell stage but GatherSampleEvidence) --------------------------------------

Outputs = Mapping[str, str | list[str]]
# called with the copied outputs once the workflow has succeeded; raising fails the stage
TargetRegistrar = Callable[[Outputs], Any]


def flat_paths(outputs: Outputs) -> list[str]:
    return [p for v in outputs.values() for p in (v if isinstance(v, list) else [v])]


def check_expected_outputs() -> bool:
    return config_retrieve(['workflow', 'check_expected_outputs'], True)


def write_outputs_json(out: Path, outputs: Outputs) -> None:
    """`outputs.json` (plan D2): `{key: gs:// path}`. Written last and atomically, so it exists only when complete."""
    out.parent.mkdir(parents=True, exist_ok=True)
    tmp = out.with_name(f'.{out.name}.tmp')
    tmp.write_text(json.dumps(dict(outputs), indent=2))
    tmp.replace(out)


def dump_entry(spec: WorkflowSpec, *, service_account_json: str | None = None) -> dict[str, Any]:
    """
    Everything P3 compares with the old submission (parity/p3_compare.py), for `--dump-inputs`. Reads the secret
    from Secret Manager unless `service_account_json` is given; it's redacted either way.
    """
    prefixed, options = render(spec, service_account_json=service_account_json)
    return {
        'inputs': prefixed,
        'options': options,
        'collect': spec.collect,
        'workflow': spec.workflow,
        'dataset': spec.dataset,
        'access_level': spec.access_level,
        'output_prefix': spec.output_prefix,
        'copy_outputs': spec.copy_outputs,
        # bounds before jitter, as `utils.create_polling_intervals` gives them
        'poll': {'min': spec.poll_min_seconds, 'max': spec.poll_max_seconds},
        'time_limit_seconds': spec.time_limit_seconds,
    }


def run_target(
    name: str,
    expected: Outputs,
    build: Callable[[], WorkflowSpec],
    *,
    out: Path,
    registrar: TargetRegistrar | None,
    result_file: Path | None = None,
    context: RunContext | None = None,
) -> int:
    """
    One stage for one target (plan §5.2): reuse its outputs if they all exist (D4), otherwise run the workflow
    `build()` returns (re-attach by Submission key or submit, watch, copy to `expected`), register, and write
    `outputs.json` last. Returns the exit code (gatk_sv_nf.errors).

    `build` is only called when the workflow has to run, since building the inputs can read GCS.
    A reused target is not registered, as cpg-flow doesn't register a reused stage. A registration failure exits 1
    rather than 75: a retry would find the outputs, reuse them and still not register (cpg-flow has the same gap).
    """
    ctx = context or RunContext()
    if check_expected_outputs() and all(ctx.storage.exists(p) for p in flat_paths(expected)):
        logger.info('[%s] all expected outputs exist; reusing', name)
        write_outputs_json(out, expected)
        return EXIT_OK

    result = run(build(), context=ctx)
    if result.ok:
        assert result.outputs is not None
        try:
            if registrar is not None:
                registrar(result.outputs)
        except Exception as e:  # any failure here fails the stage, with the reason recorded
            logger.exception('[%s] registration failed', name)
            result.exit_code, result.error = EXIT_FAILED, f'registration failed: {e!r}'
        else:
            write_outputs_json(out, result.outputs)
    if result_file is not None:
        result_file.write_text(result.to_json(indent=2))
    if not result.ok:
        logger.error('[%s] %s (exit %d): %s', name, result.status, result.exit_code, result.error or result.failures)
    return result.exit_code
