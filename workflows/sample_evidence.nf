/*
 * The sample_evidence entry (plan §5.6): GatherSampleEvidence for any number of SGs, in chunks that PLAN wrote.
 * Run on its own; nothing downstream reads its channels, since evidence_qc reads the evidence from its expected
 * paths. The run fails at the end, listing them, if any SG failed.
 */
include { PREPARE_WDL } from '../modules/prepare_wdl'
include { GSE_CHUNK } from '../modules/gse_chunk'

workflow SAMPLE_EVIDENCE {
    take:
    plan // value channel: the parsed plan.json

    main:
    def plan_dir = plan.map { p -> p.plan_dir }
    // empty when PLAN found every SG's outputs already there
    def chunks = plan.flatMap { p -> p.gse_chunks }
    def bundle = PREPARE_WDL(plan_dir).map { uri -> uri.trim() }

    GSE_CHUNK(chunks, plan_dir, bundle, params.arGuid, params.register)

    GSE_CHUNK.out.status
        .toList()
        .subscribe { statuses -> reportChunks(statuses) }

    emit:
    outputs = GSE_CHUNK.out.outputs.flatten()
}

/*
 * Log every SG that failed, by chunk, and fail the run if there were any. statuses: [chunk, exit_code file,
 * results dir] per chunk.
 */
def reportChunks(statuses) {
    def failed = statuses.findAll { _chunk, exit_code, _results -> exit_code.text.trim() != '0' }
    if (!failed) {
        log.info("GatherSampleEvidence: all ${statuses.size()} chunk(s) succeeded")
        return
    }
    def slurper = new groovy.json.JsonSlurper()
    def lines = failed.collectMany { chunk, exit_code, results ->
        def bad = results.listFiles()
            .findAll { f -> f.name.endsWith('.json') }
            .collect { f -> slurper.parseText(f.text) }
            .findAll { r -> r.exit_code != 0 }
            .collect { r -> "  ${r.name}: ${r.status} (exit ${r.exit_code})${r.error ? ': ' + r.error : ''}".toString() }
        ["${chunk} exited ${exit_code.text.trim()}".toString()] + (bad ?: ['  (no per-SG results: see the task log)'])
    }
    fail("GatherSampleEvidence failed in ${failed.size()} of ${statuses.size()} chunk(s). A relaunch reruns only the failed SGs:\n${lines.join('\n')}")
}

// error() inside a channel operator reaches the console only as "Unexpected error", so log the reason first
def fail(message) {
    log.error(message)
    error(message)
}
