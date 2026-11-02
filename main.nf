/*
 * GATK-SV through Cromwell (NEXTFLOW_PORT_PLAN.md). PLAN resolves the config, queries metamist and writes the
 * plan; params.entry picks which part of the pipeline runs from it. Each stage's logic (expected outputs, Cromwell
 * inputs, the skip check, registration) is in the nf-gatk-sv CLI, which these processes call.
 */

include { PLAN } from './modules/plan'
include { SAMPLE_EVIDENCE } from './workflows/sample_evidence'
include { EVIDENCE_QC } from './workflows/evidence_qc'

/*
 * Types only: the defaults are in nextflow.config. Nextflow converts command-line values to these types, and
 * fails the run, before any process starts, if a param without `?` has no value.
 */
params {
    arGuid: String
    dataset: String
    config: String
    entry: String

    billingProject: String?
    planOnly: Boolean
    register: Boolean

    gse_chunk_size: Integer
    gse_max_chunks: Integer

    driver_image: String?
    cromwell_task_hours: Integer
}

workflow {
    def ENTRIES = ['sample_evidence', 'evidence_qc', 'multi_sample']
    def IMPLEMENTED = ['sample_evidence', 'evidence_qc']
    if (!(params.entry in ENTRIES)) {
        error("--entry must be one of ${ENTRIES.join(', ')}, got '${params.entry}'")
    }
    if (!params.planOnly && !(params.entry in IMPLEMENTED)) {
        error("--entry ${params.entry} isn't implemented yet; only --planOnly is available for it")
    }
    if (!params.driver_image) {
        error('--driver_image is required: the image with the nf-gatk-sv CLI')
    }

    // PLAN takes only plain values, so its output is a value channel; parse it once
    def plan = PLAN(
        params.entry,
        params.config,
        params.dataset,
        params.billingProject ?: '',
        params.driver_image,
        params.gse_chunk_size,
    )
        .map { json -> new groovy.json.JsonSlurper().parseText(json.text) }

    if (params.planOnly) {
        plan.subscribe { p -> log.info("Plan written to ${p.plan_dir} (--planOnly: stopping here)") }
    }
    else if (params.entry == 'sample_evidence') {
        SAMPLE_EVIDENCE(plan)
    }
    else if (params.entry == 'evidence_qc') {
        EVIDENCE_QC(plan)
    }
}
