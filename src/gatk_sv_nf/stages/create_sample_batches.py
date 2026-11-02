"""
CreateSampleBatches (plan §5.3, Phase 3): split the multicohort's SGs into batches from the EvidenceQC QC tables.

Port of `cpg_flow_gatk_sv.singlesample_workflow.CreateSampleBatches` and `cpg_flow_gatk_sv.jobs.CreateSampleBatches`.
A light Python step, so it runs in the driver image: the copied `scripts/sample_batching.py` is called in-process
(cpg-flow runs the same script as a Batch job in the driver image).

    expected output: B(mc.analysis_dataset)/gatk_sv/<output_version>/CreateSampleBatches/sgid_batches.json
    also written:    B(mc.analysis_dataset, 'tmp')/gatk_sv/<output_version>/CreateSampleBatches/sgs_meta.json
"""

import json
import logging
import tempfile
from collections.abc import Callable, Mapping, Sequence
from pathlib import Path
from typing import Any

from cpg_utils.config import config_retrieve

from cpg_cromwell import EXIT_FAILED, EXIT_OK, RunContext, SpecError
from gatk_sv_nf import paths, register
from gatk_sv_nf.manifest import Manifest
from gatk_sv_nf.scripts import sample_batching
from gatk_sv_nf.stages import evidence_qc
from gatk_sv_nf.stages._cromwell_base import check_expected_outputs, write_outputs_json

logger = logging.getLogger(__name__)

STAGE_NAME = 'CreateSampleBatches'
ANALYSIS_TYPE = 'sv'
# cpg-flow's expected output is a bare path; outputs.json needs a key (plan D2)
OUTPUT_KEY = 'sgid_batches'
DEFAULT_MIN_BATCH_SIZE = 100  # the code default; defaults.toml sets 10, as config_template.toml does


class TooFewSequencingGroupsError(SpecError):
    """Fewer SGs than `workflow.min_batch_size`: no batches can be made."""


def expected_outputs(manifest: Manifest) -> dict[str, str]:
    prefix = paths.multicohort_stage_prefix(manifest.analysis_dataset, manifest.output_version, STAGE_NAME)
    return {OUTPUT_KEY: f'{prefix}/sgid_batches.json'}


def sgs_meta_path(manifest: Manifest) -> str:
    """`self.tmp_prefix / 'sgs_meta.json'`."""
    prefix = paths.multicohort_stage_prefix(manifest.analysis_dataset, manifest.output_version, STAGE_NAME, 'tmp')
    return f'{prefix}/sgs_meta.json'


def sgs_meta(manifest: Manifest) -> dict[str, dict[str, Any]]:
    """`{sg.id: sg.meta}` over `MultiCohort.get_sequencing_groups()`: every SG, by dataset, then insertion order."""
    return {sg.id: sg.meta for sg in manifest.sequencing_groups}


def check_batch_size(manifest: Manifest) -> None:
    """cpg-flow checks this while queueing, before EvidenceQC runs; `PLAN` does too (plan §5.4)."""
    n_sgs = len(manifest.sequencing_groups)
    min_batch_size = config_retrieve(['workflow', 'min_batch_size'], DEFAULT_MIN_BATCH_SIZE)
    if n_sgs < min_batch_size:
        raise TooFewSequencingGroupsError(
            f'too few samples to create batches: {n_sgs} sequencing group(s), workflow.min_batch_size is '
            f'{min_batch_size}'
        )


def qc_tables(manifest: Manifest) -> list[str]:
    """Each cohort's EvidenceQC `qc_table`, in manifest cohort order (never channel order, plan §5.2)."""
    return [evidence_qc.expected_outputs(c)['qc_table'] for c in manifest.cohorts]


def check_upstream(manifest: Manifest, upstream: Sequence[Mapping[str, Any]]) -> None:
    """
    The EvidenceQC `outputs.json` files Nextflow handed over must be exactly this multicohort's cohorts. They only
    order the DAG: the tables are read from their expected paths, in manifest order.
    """
    expected = set(qc_tables(manifest))
    given = [str(o.get('qc_table')) for o in upstream]
    if set(given) != expected or len(given) != len(expected):
        raise SpecError(
            f'EvidenceQC outputs given ({sorted(given)}) are not one per cohort in the manifest ({sorted(expected)})'
        )


def job_attrs(manifest: Manifest) -> dict[str, Any]:
    """Port of cpg_flow `MultiCohort.get_job_attrs`."""
    return {
        'sequencing_groups': [sg.id for sg in manifest.sequencing_groups],
        'datasets': manifest.dataset_names,
        'cohorts': manifest.cohort_ids,
    }


def register_outputs(manifest: Manifest, outputs: Mapping[str, Any], **kwargs: Any) -> int:
    """
    One `sv` analysis (sgid_batches.json), linked to every cohort, in the analysis dataset. The meta keeps
    `datasets` and `cohorts`; `sequencing_groups` is dropped by the registration, as in cpg-flow.
    """
    return register.register_analysis(
        output=str(outputs[OUTPUT_KEY]),
        analysis_type=ANALYSIS_TYPE,
        dataset=manifest.analysis_dataset,
        meta=register.stage_job_attrs(STAGE_NAME, job_attrs(manifest)),
        cohort_ids=manifest.cohort_ids,
        **kwargs,
    )


Registrar = Callable[[Manifest, Mapping[str, Any]], Any]


def run(
    manifest: Manifest,
    *,
    out: Path,
    upstream: Sequence[Mapping[str, Any]] | None = None,
    registrar: Registrar | None = register_outputs,
    context: RunContext | None = None,
) -> int:
    """
    Reuse `sgid_batches.json` if it exists (D4); otherwise check the batch size, write `sgs_meta.json`, run the
    batching on the QC tables, write the result to its expected path, register it, and write `outputs.json` last.
    """
    ctx = (context or RunContext()).resolved()
    storage = ctx.storage
    assert storage is not None
    expected = expected_outputs(manifest)
    output = expected[OUTPUT_KEY]
    if upstream is not None:
        check_upstream(manifest, upstream)

    if check_expected_outputs() and storage.exists(output):
        logger.info('[%s] %s exists; reusing', STAGE_NAME, output)
        write_outputs_json(out, expected)
        return EXIT_OK

    check_batch_size(manifest)
    meta = sgs_meta(manifest)
    meta_json = json.dumps(meta)  # json.dump(sequencing_groups, f), as cpg-flow writes it
    storage.write(sgs_meta_path(manifest), meta_json)

    with tempfile.TemporaryDirectory(prefix='create-sample-batches-') as tmp:
        local = Path(tmp)
        local_tables = []
        for i, uri in enumerate(qc_tables(manifest)):
            if (blob := storage.read(uri)) is None:
                raise SpecError(f'EvidenceQC QC table not found: {uri}')
            table = local / f'qc_table_{i}.tsv'
            table.write_bytes(blob.data)
            local_tables.append(str(table))
        (local / 'sgs_meta.json').write_text(meta_json)
        sample_batching.partition_batches(
            metadata_files=local_tables,
            sequencing_groups_json=str(local / 'sgs_meta.json'),
            output_json=str(local / 'sgid_batches.json'),
        )
        storage.write(output, (local / 'sgid_batches.json').read_bytes())

    if registrar is not None:
        try:
            registrar(manifest, expected)
        except Exception:  # the outputs exist; a rerun reuses them and won't register (as cpg-flow)
            logger.exception('[%s] registration failed', STAGE_NAME)
            return EXIT_FAILED
    write_outputs_json(out, expected)
    logger.info('[%s] wrote %s', STAGE_NAME, output)
    return EXIT_OK
