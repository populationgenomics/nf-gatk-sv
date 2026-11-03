/*
 * GatherSampleEvidence for one chunk of SGs (plan §5.6): one Cromwell workflow per SG, submitted (or re-attached),
 * watched and collected in this one task. As each SG finishes, its caller VCFs are registered and
 * outputs/<sg>.json is written. SGs whose outputs already exist are reused, and nothing is submitted for them.
 *
 * One failing SG never stops the others, in this chunk or any other: the task records the CLI's exit code and
 * succeeds, and the workflow reports every failed SG once all chunks have run. Only a transient error (75, retried)
 * or a spec, config or signal exit fails the task itself (conf/base.config).
 */
process GSE_CHUNK {
    tag "${chunk.tokenize('/').last()}"
    label 'driver'
    // Never replayed by -resume. A chunk whose SGs failed still ends the task successfully (exit 1 is reported at
    // the end of the run), so a cached copy would skip exactly the SGs a relaunch is meant to retry. Nothing is
    // lost: PLAN only chunks SGs whose outputs don't exist yet, and in-flight workflows are re-attached
    cache false

    input:
    val chunk    // gs:// JSON list of SG IDs, written by PLAN
    val plan_dir
    val bundle   // from PREPARE_WDL
    val ar_guid  // the ar-guid billing label on every Cromwell workflow
    val register

    output:
    path 'outputs/*.json', optional: true, emit: outputs
    // the CLI's exit code, and one workflow report per submitted or re-attached workflow
    tuple val(chunk), path('exit_code'), path('results'), emit: status

    script:
    def no_register = register ? '' : '--no-register'
    """
    mkdir -p outputs results
    rc=0
    nf-gatk-sv stage GatherSampleEvidence \\
        --plan ${plan_dir} \\
        --chunk ${chunk} \\
        --ar-guid ${ar_guid} \\
        --bundle ${bundle} \\
        --out-dir outputs \\
        --results-dir results \\
        ${no_register} || rc=\$?
    echo \$rc > exit_code

    # 1: some SGs failed. 124: some hit the time limit, and were aborted. Both are reported at the end of the run
    case \$rc in
        0|1|124) exit 0 ;;
        *) exit \$rc ;;
    esac
    """

    stub:
    // ext.stub_exit_code: what the CLI returns, so the end-of-run report can be exercised
    def rc = task.ext.stub_exit_code ?: 0
    """
    mkdir -p outputs results
    echo '{}' > outputs/stub.json
    if [ ${rc} -ne 0 ]; then
        echo '{"name": "GatherSampleEvidence/stub-ds/STUB1", "status": "Failed", "exit_code": ${rc}, "error": "stub failure", "failures": []}' > results/stub.json
    fi
    echo ${rc} > exit_code
    """
}
