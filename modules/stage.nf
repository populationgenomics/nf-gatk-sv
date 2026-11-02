/*
 * One stage for one target (plan §5.2, §13). The nf-gatk-sv CLI does everything: the skip check (D4), the Cromwell
 * inputs or the local steps, registration, and outputs.json ({key: gs:// path}, plan D2) last. Include these
 * under the stage's own name, e.g. `include { STAGE as EVIDENCE_QC } from '../modules/stage'`.
 *
 * Upstream outputs arrive as small outputs.json files, each paired with the name of the stage that wrote it, and
 * reach the CLI as `--upstream Stage=<file>`. The CLI orders them by the manifest, never by channel order.
 */

// [stage names], [outputs.json files] -> '--upstream Stage=file ...'. One staged file comes through as a bare path
def upstreamArgs(stages, files) {
    def fileList = files instanceof List ? files : [files]
    if (stages.size() != fileList.size()) {
        error("upstream: ${stages.size()} stage name(s) for ${fileList.size()} file(s)")
    }
    [stages, fileList].transpose().collect { s, f -> "--upstream ${s}=${f}" }.join(' ')
}

/*
 * A Cromwell stage: submit (or re-attach to) one workflow, watch it and copy its outputs. Exit codes are
 * cpg-cromwell's: 75 (transient) is retried and re-attaches; anything else fails the task (conf/base.config).
 */
process STAGE {
    tag "${stage_name}/${target}"
    label 'driver'
    label 'cromwell'

    input:
    val stage_name
    tuple val(target), val(upstream_stages), path(upstream, stageAs: 'upstream/*/outputs.json')
    val plan_dir
    val bundle   // from PREPARE_WDL
    val ar_guid  // the ar-guid billing label on the Cromwell workflow
    val register

    output:
    tuple val(target), path('outputs.json'), emit: outputs
    path 'result.json', optional: true, emit: result

    script:
    def no_register = register ? '' : '--no-register'
    """
    nf-gatk-sv stage ${stage_name} \\
        --plan ${plan_dir} \\
        --target ${target} \\
        --ar-guid ${ar_guid} \\
        --bundle ${bundle} \\
        ${upstreamArgs(upstream_stages, upstream)} \\
        --result result.json \\
        --out outputs.json \\
        ${no_register}
    """

    stub:
    """
    echo '{"stub": "${stage_name}/${target}"}' > outputs.json
    """
}

/*
 * A light Python stage in the driver image (CreateSampleBatches, and later the PED and ploidy steps): no Cromwell,
 * so no bundle, billing label or target ID: the multicohort is the only target these have.
 */
process PY_STAGE {
    tag "${stage_name}"
    label 'driver'

    input:
    val stage_name
    tuple val(upstream_stages), path(upstream, stageAs: 'upstream/*/outputs.json')
    val plan_dir
    val register

    output:
    path 'outputs.json', emit: outputs

    script:
    def no_register = register ? '' : '--no-register'
    """
    nf-gatk-sv stage ${stage_name} \\
        --plan ${plan_dir} \\
        ${upstreamArgs(upstream_stages, upstream)} \\
        --out outputs.json \\
        ${no_register}
    """

    stub:
    """
    echo '{"stub": "${stage_name}"}' > outputs.json
    """
}
