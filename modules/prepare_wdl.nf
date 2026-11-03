/*
 * Bundle the gatk-sv WDL at workflow.gatk_sv_commit, once per run, and emit the bundle's URI (plan §5.1). Every
 * Cromwell stage submits from it. An existing bundle is reused. For non-test access levels the commit must be on
 * main, which `nf-gatk-sv prepare-wdl` checks here and the stages check again before submitting.
 */
process PREPARE_WDL {
    label 'driver'

    input:
    val plan_dir

    output:
    stdout

    script:
    """
    nf-gatk-sv prepare-wdl --plan ${plan_dir}
    """

    stub:
    """
    echo 'gs://stub-tmp/gatk_sv_nf/bundles/gatk-sv/stub-commit/'
    """
}
