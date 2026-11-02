/*
 * The evidence_qc entry (plan §13, Phase 3): EvidenceQC per cohort, then CreateSampleBatches over the multicohort.
 * EvidenceQC reads each SG's GatherSampleEvidence outputs from their cpg-flow paths, since sample_evidence ran as
 * a separate launch; PLAN has already checked they all exist, and that there are enough SGs to batch.
 */
include { PREPARE_WDL } from '../modules/prepare_wdl'
include { STAGE as EVIDENCE_QC_STAGE } from '../modules/stage'
include { PY_STAGE as CREATE_SAMPLE_BATCHES } from '../modules/stage'

workflow EVIDENCE_QC {
    take:
    plan // value channel: the parsed plan.json

    main:
    def plan_dir = plan.map { p -> p.plan_dir }
    def bundle = PREPARE_WDL(plan_dir).map { uri -> uri.trim() }
    // one task per cohort; no upstream outputs.json (the evidence is read from its expected paths)
    def cohorts = plan.flatMap { p -> p.cohort_ids }.map { cohort -> tuple(cohort, [], []) }

    EVIDENCE_QC_STAGE('EvidenceQC', cohorts, plan_dir, bundle, params.arGuid, params.register)

    // every cohort's outputs.json, in whatever order they finished: the CLI orders them by the manifest
    def qc = EVIDENCE_QC_STAGE.out.outputs
        .map { _cohort, outputs -> outputs }
        .collect()
        .map { files -> tuple(files.collect { 'EvidenceQC' }, files) }

    CREATE_SAMPLE_BATCHES('CreateSampleBatches', qc, plan_dir, params.register)

    emit:
    evidence_qc = EVIDENCE_QC_STAGE.out.outputs
    sample_batches = CREATE_SAMPLE_BATCHES.out.outputs
}
