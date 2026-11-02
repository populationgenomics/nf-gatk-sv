/*
 * The first task of every run (plan §5.4): resolve the config, query metamist, write the content-addressed plan dir
 * and emit plan.json, from which main.nf builds the rest of the DAG. Never cached: metamist state such as active
 * flags and the latest spicy VCF can change between launches. arGuid is never an input, so an unchanged plan keeps
 * its plan_dir, and the tasks downstream of it keep their -resume cache hits.
 */
process PLAN {
    tag "${entry}"
    label 'driver'
    cache false

    input:
    val entry
    val config         // gs:// path of the run TOML
    val dataset        // becomes workflow.dataset
    val billing_project // empty: PLAN reads it from the VM's metadata server
    val driver_image   // becomes workflow.driver_image
    val gse_chunk_size

    output:
    path 'plan.json'

    script:
    def billing = billing_project ? "--billing-project ${billing_project}" : ''
    def image = driver_image ? "--driver-image ${driver_image}" : ''
    """
    nf-gatk-sv plan \\
        --entry ${entry} \\
        --config ${config} \\
        --dataset ${dataset} \\
        ${billing} \\
        ${image} \\
        --gse-chunk-size ${gse_chunk_size} \\
        --out plan.json
    """

    stub:
    // stub runs have no metamist, so a test config hands over a prepared plan.json (task.ext.stub_plan)
    """
    cp ${task.ext.stub_plan} plan.json
    """
}
