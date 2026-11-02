/*
 * Bundle the gatk-sv WDL at workflow.gatk_sv_commit, once per run, and emit the bundle's URI (plan §5.1). Every
 * Cromwell stage submits from it. An existing bundle is reused. For non-test access levels the commit must be on
 * main, which cpg-cromwell checks here and again at submission.
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
