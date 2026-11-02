"""
Builds a `cpg_cromwell.WorkflowSpec` for a GATK-SV stage (plan §5.1).

Port of `cpg_flow_gatk_sv.utils.add_gatk_sv_jobs`, `create_polling_intervals` and
`CromwellJobSizes`. Submitting, watching, re-attaching and copying outputs are all cpg-cromwell's.
"""

import enum
from collections.abc import Mapping
from typing import Any

from cpg_utils.config import config_retrieve

from cpg_cromwell import PollPolicy, WorkflowSource, WorkflowSpec
from cpg_cromwell.bundle import bundle_uri
from gatk_sv_nf.paths import analysis_dataset_tmp

GATK_SV_REPO = 'gatk-sv'
MAX_CONSECUTIVE_ERRORS = 25


class CromwellJobSize(enum.Enum):
    SMALL = 'small'
    MEDIUM = 'medium'
    LARGE = 'large'


DEFAULT_POLLING_INTERVALS: dict[CromwellJobSize, dict[str, int]] = {
    CromwellJobSize.SMALL: {'min': 30, 'max': 140},
    CromwellJobSize.MEDIUM: {'min': 40, 'max': 400},
    CromwellJobSize.LARGE: {'min': 200, 'max': 2000},
}


def poll_policy(size: CromwellJobSize) -> PollPolicy:
    """
    Per-size polling bounds, overridable from `[cromwell_polling_intervals.<size>]`.
    `jitter=True` gives `randint(min, 2*min)` / `randint(max, 2*max)` once per workflow, as before.
    """
    bounds = dict(DEFAULT_POLLING_INTERVALS[size])
    if override := config_retrieve(['cromwell_polling_intervals', size.value], None):
        bounds.update(override)
    return PollPolicy(
        min_seconds=bounds['min'],
        max_seconds=bounds['max'],
        jitter=True,
        max_consecutive_errors=MAX_CONSECUTIVE_ERRORS,
        time_limit_seconds=config_retrieve(['workflow', 'time_limit_seconds'], None),
    )


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


def default_reattach_root() -> str:
    return analysis_dataset_tmp('gatk_sv_nf/cromwell_ids')


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
    return WorkflowSpec(
        name=output_prefix.removeprefix('gatk_sv/output/'),
        source=WorkflowSource(bundle=bundle or default_bundle(), workflow=f'{wfl_name}.wdl'),
        inputs=apply_resource_overrides(wfl_name, dict(inputs)),
        input_prefix=wfl_name,
        dataset=config_retrieve(['workflow', 'dataset']),
        access_level=config_retrieve(['workflow', 'access_level']),
        output_prefix=output_prefix,
        ar_guid=ar_guid,
        labels=dict(labels),
        copy_outputs=config_retrieve(['workflow', 'copy_outputs'], False),
        collect=dict(collect),
        poll=poll_policy(size),
    )
