"""
EvidenceQC (plan §9, Phase 3): one Cromwell workflow per cohort.

Port of `cpg_flow_gatk_sv.singlesample_workflow.EvidenceQC` and `cpg_flow_gatk_sv.jobs.EvidenceQC`. It reads each
SG's GatherSampleEvidence outputs from their cpg-flow paths, not from channels, because the sample_evidence entry
runs as a separate launch. `PLAN` checks they all exist before an evidence_qc run (plan §13).
"""

from collections.abc import Mapping
from typing import Any

from cpg_utils.config import config_retrieve

from gatk_sv_nf import paths, register
from gatk_sv_nf.cromwell import WorkflowSpec
from gatk_sv_nf.manifest import Cohort, Manifest
from gatk_sv_nf.stages import gather_sample_evidence as gse
from gatk_sv_nf.stages._cromwell_base import CromwellJobSize, build_spec

STAGE_NAME = 'EvidenceQC'
ANALYSIS_TYPE = 'qc'
ANALYSIS_KEYS = ('qc_table',)


def expected_outputs(cohort: Cohort) -> dict[str, str]:
    """Batch-level summary files, under `B(cohort.dataset)/gatk_sv/<cohort_id>/EvidenceQC/`."""
    fname_by_key = {
        'ploidy_matrix': 'ploidy_matrix.bed.gz',
        'ploidy_plots': 'ploidy_plots.tar.gz',
        'WGD_dist': 'WGD_score_distributions.pdf',
        'WGD_matrix': 'WGD_scoring_matrix_output.bed.gz',
        'WGD_scores': 'WGD_scores.txt.gz',
        'bincov_matrix': f'{STAGE_NAME}.RD.txt.gz',
        'bincov_matrix_index': f'{STAGE_NAME}.RD.txt.gz.tbi',
        'bincov_median': 'medianCov.transposed.bed',
        'qc_table': 'evidence_qc_table.tsv',
    }
    for caller in gse.SV_CALLERS:
        for k in ['low', 'high']:
            fname_by_key[f'{caller}_qc_{k}'] = f'{caller}_QC.outlier.{k}'

    prefix = paths.cohort_stage_prefix(cohort.dataset, cohort.id, STAGE_NAME)
    return {key: f'{prefix}/{fname}' for key, fname in fname_by_key.items()}


def inputs(manifest: Manifest, cohort: Cohort) -> dict[str, Any]:
    """
    Unprefixed Cromwell inputs, before resource overrides. `samples` is every SG in the cohort, in cohort order
    (cpg-flow's `cohort.get_sequencing_group_ids()`: its `active` flag is never cleared in this workflow).

    The caller VCFs are read for every caller whatever `only_jobs` says. cpg-flow takes them from
    GatherSampleEvidence's `only_jobs`-filtered expected outputs, so with `only_jobs` set it fails with a KeyError
    instead; with it unset, which is the only case that runs, the two are the same.
    """
    sgs = manifest.cohort_sequencing_groups(cohort.id)
    evidence = {sg.id: gse.all_outputs(sg) for sg in sgs}
    sgids = [sg.id for sg in sgs]

    input_dict: dict[str, Any] = {
        'batch': cohort.id,
        'samples': sgids,
        'run_vcf_qc': True,
        'counts': [evidence[sid]['coverage_counts'] for sid in sgids],
        'sv_base_docker': config_retrieve(['images', 'sv_base_docker']),
        'sv_pipeline_qc_docker': config_retrieve(['images', 'sv_pipeline_qc_docker']),
        'sv_base_mini_docker': config_retrieve(['images', 'sv_base_mini_docker']),
        'sv_pipeline_docker': config_retrieve(['images', 'sv_pipeline_docker']),
        'genome_file': config_retrieve(['references', 'wgd_scoring_mask']),
    }
    for caller in gse.SV_CALLERS:
        input_dict[f'{caller}_vcfs'] = [evidence[sid][f'{caller}_vcf'] for sid in sgids]
    return input_dict


def labels() -> dict[str, str]:
    """Billing labels; `ar-guid` goes through WorkflowSpec.ar_guid, not here."""
    return {'stage': STAGE_NAME.lower()}


def build_cohort_spec(manifest: Manifest, cohort: Cohort, *, ar_guid: str, bundle: str | None = None) -> WorkflowSpec:
    # runs for approx 5 hours, depending on sample count
    return build_spec(
        wfl_name=STAGE_NAME,
        target_dataset=cohort.dataset,
        inputs=inputs(manifest, cohort),
        collect=expected_outputs(cohort),
        labels=labels(),
        ar_guid=ar_guid,
        size=CromwellJobSize.MEDIUM,
        bundle=bundle,
    )


def register_outputs(cohort: Cohort, outputs: Mapping[str, Any], **kwargs: Any) -> list[int]:
    """
    One `qc` analysis (the QC table), linked to the cohort, in the cohort's dataset. The meta is the stage's job
    attributes; the cohort's own (`sequencing_groups`) are dropped by the registration, as in cpg-flow.
    `kwargs` go to `register.register_analysis` (`create`, `size_of`; for tests).
    """
    meta = register.stage_job_attrs(STAGE_NAME, {'sequencing_groups': list(cohort.sequencing_group_ids)})
    return [
        register.register_analysis(
            output=str(outputs[key]),
            analysis_type=ANALYSIS_TYPE,
            dataset=cohort.dataset,
            meta=meta,
            cohort_ids=[cohort.id],
            **kwargs,
        )
        for key in ANALYSIS_KEYS
    ]
