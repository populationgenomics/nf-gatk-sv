"""
cpg-flow-compatible output path layout (plan §6).

Paths are built as strings (no cloudpathlib), joined
the way `to_path(dataset_path(...)) / 'x'` would join them.
"""

from cpg_utils.config import config_retrieve, dataset_path

WORKFLOW_NAME = 'gatk_sv'  # `WF` in plan §6; only parity runs override it


def sequencing_subdir() -> str:
    """Port of `cpg_flow.targets.helpers.sequencing_subdir`: '' for short-read genomes."""
    seq_type = config_retrieve(['workflow', 'sequencing_type'], None)
    seq_tech = config_retrieve(['workflow', 'sequencing_technology'], None)
    if seq_tech:
        seq_tech = seq_tech.replace('-', '_')
    if not seq_tech or seq_tech == 'short_read':
        return '' if not seq_type or seq_type == 'genome' else seq_type
    return f'{seq_tech}/{seq_type}' if seq_type != 'genome' else seq_tech


def dataset_prefix(dataset: str, category: str | None = None) -> str:
    """`B(ds, category)` in plan §6, i.e. cpg_flow `Dataset.prefix()`, without a trailing slash."""
    return dataset_path(sequencing_subdir(), category=category, dataset=dataset).rstrip('/')


def sv_evidence_prefix(dataset: str) -> str:
    """cpg_flow `SequencingGroup.make_sv_evidence_path`: `B(sg.dataset)/sv_evidence`."""
    return f'{dataset_prefix(dataset)}/sv_evidence'


def analysis_dataset_tmp(suffix: str) -> str:
    """`gs://<tmp bucket of workflow.dataset>/<suffix>`: where the port keeps its own state."""
    return dataset_path(suffix, category='tmp', dataset=config_retrieve(['workflow', 'dataset']))


def cohort_stage_prefix(cohort_dataset: str, cohort_id: str, stage: str, category: str | None = None) -> str:
    """`Stage.get_stage_cohort_prefix`: `B(cohort.dataset)/WF/<cohort_id>/<Stage>`."""
    return f'{dataset_prefix(cohort_dataset, category)}/{WORKFLOW_NAME}/{cohort_id}/{stage}'


def multicohort_stage_prefix(
    analysis_dataset: str, output_version: str, stage: str, category: str | None = None
) -> str:
    """`Stage.prefix` (and `tmp_prefix` etc.): `B(mc.analysis_dataset)/WF/<output_version>/<Stage>`."""
    return f'{dataset_prefix(analysis_dataset, category)}/{WORKFLOW_NAME}/{output_version}/{stage}'


def cohort_ped(cohort_dataset: str, cohort_id: str) -> str:
    """MakeCohortCombinedPed's output."""
    return f'{cohort_stage_prefix(cohort_dataset, cohort_id, "MakeCohortCombinedPed")}/ped_with_ref_panel.ped'


def multicohort_ped(analysis_dataset: str, output_version: str) -> str:
    """MakeMultiCohortCombinedPed's output."""
    prefix = multicohort_stage_prefix(analysis_dataset, output_version, 'MakeMultiCohortCombinedPed')
    return f'{prefix}/ped_with_ref_panel.ped'


def dataset_sgid_list(dataset: str, output_version: str) -> str:
    """`utils.write_dataset_sg_ids`: the dataset's active SG IDs."""
    return f'{dataset_prefix(dataset, "tmp")}/{output_version}/sv-sgid-list.txt'


def multicohort_sgid_list(analysis_dataset: str, output_version: str) -> str:
    """`utils.make_combined_sgid_file`: every active SG ID in the multicohort, sorted."""
    return f'{dataset_prefix(analysis_dataset, "tmp")}/{output_version}/mc-sv-sgid-list.txt'


def plan_root() -> str:
    """`gs://<tmp of workflow.dataset>/gatk_sv_nf/plans`: one content-addressed dir per plan (plan §5.4)."""
    return analysis_dataset_tmp('gatk_sv_nf/plans')
