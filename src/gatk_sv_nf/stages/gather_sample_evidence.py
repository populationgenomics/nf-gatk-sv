"""
GatherSampleEvidence (plan §5.6, §9).

Port of `cpg_flow_gatk_sv.singlesample_workflow.GatherSampleEvidence` and
`cpg_flow_gatk_sv.jobs.GatherSampleEvidence`. One workflow per SG; a chunk of SGs runs in one
process through `gatk_sv_nf.cromwell.run_many` (`cpg_utils.cromwell_client.run_workflows`), which
re-attaches or submits, watches and copies the outputs, one thread per SG. As each SG's outputs land,
`on_result` registers its caller VCFs in metamist and writes `<out_dir>/<sg_id>.json`.
"""

import json
import logging
import posixpath
from collections.abc import Callable, Iterable, Mapping
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from cpg_utils.config import config_retrieve

from gatk_sv_nf import register
from gatk_sv_nf.cromwell import RunContext, WorkflowReport, WorkflowSpec, run_many, write_reports
from gatk_sv_nf.errors import EXIT_OK, SpecError, combine_exit_codes
from gatk_sv_nf.manifest import SequencingGroup
from gatk_sv_nf.paths import sv_evidence_prefix
from gatk_sv_nf.stages._cromwell_base import CromwellJobSize, build_spec

logger = logging.getLogger(__name__)

STAGE_NAME = 'GatherSampleEvidence'
SV_CALLERS = ('manta', 'wham', 'scramble')
ANALYSIS_TYPE = 'sv'

Exists = Callable[[str], bool]


def only_jobs() -> list[str] | None:
    """`workflow.GatherSampleEvidence.only_jobs`, copied so callers can't mutate the config."""
    jobs = config_retrieve(['workflow', STAGE_NAME, 'only_jobs'], None)
    return list(jobs) if jobs else None


def sv_callers(add_manta: bool = False) -> list[str]:
    """Port of `utils.get_sv_callers`: the callers `only_jobs` enables (all of them when unset)."""
    if jobs := only_jobs():
        callers = [caller for caller in SV_CALLERS if caller in jobs]
        if add_manta and 'scramble' in jobs and 'manta' not in jobs:
            callers.append('manta')
        if not callers:
            logger.warning('No SV callers enabled')
        return callers
    return list(SV_CALLERS)


def analysis_keys() -> list[str]:
    """The stage's `analysis_keys`: one `sv` analysis per caller VCF (no add_manta)."""
    return [f'{caller}_vcf' for caller in sv_callers()]


def all_outputs(sg: SequencingGroup) -> dict[str, str]:
    """
    Every output GatherSampleEvidence can produce for the SG, whatever `only_jobs` says. This is what the later
    stages read (EvidenceQC: `inputs.as_dict_by_target(GatherSampleEvidence)` with only_jobs unset).

    Evidence names must end exactly in `.sr.txt.gz`, `.pe.txt.gz` and `.sd.txt.gz`: GATK
    PrintSVEvidence picks its codec from the file name.
    """
    prefix = sv_evidence_prefix(sg.dataset)
    outputs = {
        'coverage_counts': f'{prefix}/{sg.id}.coverage_counts.tsv.gz',
        # split reads
        'pesr_split': f'{prefix}/{sg.id}.sr.txt.gz',
        'pesr_split_index': f'{prefix}/{sg.id}.sr.txt.gz.tbi',
        # site depth
        'pesr_sd': f'{prefix}/{sg.id}.sd.txt.gz',
        'pesr_sd_index': f'{prefix}/{sg.id}.sd.txt.gz.tbi',
        # discordant paired reads
        'pesr_disc': f'{prefix}/{sg.id}.pe.txt.gz',
        'pesr_disc_index': f'{prefix}/{sg.id}.pe.txt.gz.tbi',
    }
    for caller in SV_CALLERS:
        outputs[f'{caller}_vcf'] = f'{prefix}/{sg.id}.{caller}.vcf.gz'
        outputs[f'{caller}_index'] = f'{prefix}/{sg.id}.{caller}.vcf.gz.tbi'
    return outputs


def _caller_of(key: str) -> str | None:
    """The caller a `<caller>_vcf` / `<caller>_index` key belongs to; None for the evidence keys."""
    return next((c for c in SV_CALLERS if key in (f'{c}_vcf', f'{c}_index')), None)


def expected_outputs(sg: SequencingGroup) -> dict[str, str]:
    """The stage's expected outputs: `all_outputs`, cut down to the jobs `only_jobs` lists."""
    callers = sv_callers(add_manta=True)
    outputs = {key: path for key, path in all_outputs(sg).items() if _caller_of(key) in (None, *callers)}

    if jobs := only_jobs():
        # keep the outputs of the listed jobs only (substring match on the key, as cpg-flow does)
        kept = {key: path for job in jobs for key, path in outputs.items() if job in key}
        if 'scramble' in jobs and 'manta' not in jobs:
            # Scramble needs a Manta VCF: expect it, so it's either reused or produced
            kept['manta_vcf'] = outputs['manta_vcf']
            kept['manta_index'] = outputs['manta_index']
        outputs = kept
    return outputs


def evidence_qc_inputs(sg: SequencingGroup) -> list[str]:
    """
    What EvidenceQC reads for each SG (`jobs/EvidenceQC.py`): the coverage counts and every caller's VCF, whatever
    `only_jobs` says. `PLAN` checks these exist before an evidence_qc run (plan §13).
    """
    every = all_outputs(sg)
    return [every['coverage_counts'], *(every[f'{c}_vcf'] for c in SV_CALLERS)]


def _reference_dict(fasta: str) -> str:
    """`to_path(fasta).with_suffix('.dict')` without constructing a cloud path."""
    return posixpath.splitext(fasta)[0] + '.dict'


def inputs(sg: SequencingGroup, expected: Mapping[str, str], exists: Exists) -> tuple[dict[str, Any], dict[str, str]]:
    """
    Unprefixed Cromwell inputs (before resource overrides) and the outputs to collect.

    `collect` is `expected` minus the Manta VCF and index when they are reused as inputs (scramble
    without manta, Manta VCF already present).
    """
    if not sg.cram:
        raise SpecError(f'{sg.id}: no CRAM in the manifest')
    fasta = config_retrieve(['workflow', 'ref_fasta'])
    collect = dict(expected)

    input_dict: dict[str, Any] = {
        'bam_or_cram_file': sg.cram,
        'bam_or_cram_index': f'{sg.cram}.crai',
        'cloud_sdk_docker': config_retrieve(['images', 'cloud_sdk_docker']),
        'gatk_docker': config_retrieve(['images', 'gatk_docker']),
        'gatk_docker_pesr_override': config_retrieve(['images', 'gatk_docker_pesr_override']),
        'genomes_in_the_cloud_docker': config_retrieve(['images', 'genomes_in_the_cloud_docker']),
        'manta_docker': config_retrieve(['images', 'manta_docker']),
        'manta_region_bed': config_retrieve(['references', 'manta_region_bed']),
        'manta_region_bed_index': config_retrieve(['references', 'manta_region_bed_index']),
        'mei_bed': config_retrieve(['references', 'mei_bed']),
        # a cost-improvement in cloud environments
        'move_bam_or_cram_files': True,
        'preprocessed_intervals': config_retrieve(['references', 'preprocessed_intervals']),
        'primary_contigs_fai': config_retrieve(['references', 'primary_contigs_fai']),
        'primary_contigs_list': config_retrieve(['references', 'primary_contigs_list']),
        'reference_dict': _reference_dict(fasta),
        'reference_fasta': fasta,
        'reference_index': f'{fasta}.fai',
        'reference_version': '38',
        'sample_id': sg.id,
        'samtools_cloud_docker': config_retrieve(['images', 'samtools_cloud_docker']),
        'scramble_docker': config_retrieve(['images', 'scramble_docker']),
        'sd_locs_vcf': config_retrieve(['references', 'dbsnp_vcf']),
        'sv_base_mini_docker': config_retrieve(['images', 'sv_base_mini_docker']),
        'sv_pipeline_docker': config_retrieve(['images', 'sv_pipeline_docker']),
        'wham_docker': config_retrieve(['images', 'wham_docker']),
        'wham_include_list_bed_file': config_retrieve(['references', 'wham_include_list_bed_file']),
    }

    if jobs := only_jobs():
        if 'coverage_counts' not in jobs:
            input_dict['collect_coverage'] = False
        if 'pesr' not in jobs:
            input_dict['collect_pesr'] = False

        if 'scramble' in jobs and 'manta' not in jobs:
            if exists(collect['manta_vcf']):
                # reuse the existing Manta VCF, and don't expect the workflow to produce it
                input_dict['manta_vcf_input'] = collect.pop('manta_vcf')
                input_dict['manta_vcf_index_input'] = collect.pop('manta_index')
            else:
                jobs.append('manta')  # no Manta VCF to reuse: run Manta as well

        # disable the callers that aren't listed by nulling their docker image
        for caller in SV_CALLERS:
            if caller not in jobs:
                input_dict[f'{caller}_docker'] = None

    return input_dict, collect


def labels(sg: SequencingGroup) -> dict[str, str]:
    """Billing labels; `ar-guid` goes through WorkflowSpec.ar_guid, not here."""
    return {
        'dataset': sg.dataset,  # already lowercase
        'sequencing-group': sg.id.lower(),
        'stage': STAGE_NAME.lower(),
    }


def build_sg_spec(
    sg: SequencingGroup,
    *,
    ar_guid: str,
    exists: Exists,
    bundle: str | None = None,
) -> WorkflowSpec:
    input_dict, collect = inputs(sg, expected_outputs(sg), exists)
    return build_spec(
        wfl_name=STAGE_NAME,
        target_dataset=sg.dataset,
        inputs=input_dict,
        collect=collect,
        labels=labels(sg),
        ar_guid=ar_guid,
        size=CromwellJobSize.LARGE,
        sequencing_group_id=sg.id,
        bundle=bundle,
    )


def register_outputs(sg: SequencingGroup, outputs: Mapping[str, Any], **kwargs: Any) -> list[int]:
    """
    One `sv` analysis per caller VCF, linked to the SG, in the SG's dataset. `kwargs` go to
    `register.register_analysis` (`create`, `size_of`; for tests).
    """
    meta = register.stage_job_attrs(STAGE_NAME, sg.job_attrs())
    return [
        register.register_analysis(
            output=str(outputs[key]),
            analysis_type=ANALYSIS_TYPE,
            dataset=sg.dataset,
            meta=meta,
            sequencing_group_ids=[sg.id],
            **kwargs,
        )
        for key in analysis_keys()
    ]


# called once per succeeded SG, after its outputs are copied; raising fails that SG
Registrar = Callable[[SequencingGroup, Mapping[str, Any]], Any]


def _write_outputs(out_dir: Path, sg_id: str, outputs: Mapping[str, Any]) -> None:
    out_dir.mkdir(parents=True, exist_ok=True)
    tmp = out_dir / f'.{sg_id}.json.tmp'
    tmp.write_text(json.dumps(dict(outputs), indent=2))
    tmp.replace(out_dir / f'{sg_id}.json')  # atomic: the file only exists once complete


@dataclass
class ChunkReport:
    reused: list[str]
    results: list[WorkflowReport]

    @property
    def failed(self) -> list[WorkflowReport]:
        return [r for r in self.results if not r.ok]

    @property
    def exit_code(self) -> int:
        return combine_exit_codes([r.exit_code for r in self.results])


def run_chunk(
    sgs: Iterable[SequencingGroup],
    *,
    ar_guid: str,
    out_dir: Path,
    results_dir: str | None = None,
    bundle: str | None = None,
    registrar: Registrar | None = register_outputs,
    submit_interval: float = 1.0,
    max_in_flight: int | None = None,
    context: RunContext | None = None,
) -> ChunkReport:
    """
    Run GatherSampleEvidence for every SG in the chunk. One failing SG never stops the others.

    SGs whose expected outputs all exist are reused (D4): `<sg>.json` is written, nothing is
    submitted and nothing is registered, as cpg-flow does. Everything else goes to `run_many`.
    `registrar=None` skips metamist registration (`--no-register`, plan §8). With `results_dir`, one
    report per workflow is written there once they have all finished.
    """
    ctx = context or RunContext()
    exists = ctx.storage.exists
    check_existing = config_retrieve(['workflow', 'check_expected_outputs'], True)

    if registrar is not None and not analysis_keys():
        # cpg-flow refuses to queue a registering stage with no analysis_keys; fail before submitting
        raise SpecError(f'{STAGE_NAME}: only_jobs enables no SV caller, so there is nothing to register')

    reused: list[str] = []
    specs: list[WorkflowSpec] = []
    sg_by_name: dict[str, SequencingGroup] = {}
    for sg in sgs:
        expected = expected_outputs(sg)
        if check_existing and all(exists(p) for p in expected.values()):
            logger.info('[%s] all expected outputs exist; reusing', sg.id)
            _write_outputs(out_dir, sg.id, expected)
            reused.append(sg.id)
            continue
        spec = build_sg_spec(sg, ar_guid=ar_guid, exists=exists, bundle=bundle)
        specs.append(spec)
        sg_by_name[spec.name] = sg

    if not specs:
        return ChunkReport(reused=reused, results=[])

    def on_result(spec: WorkflowSpec, result: WorkflowReport) -> None:
        # as soon as this SG's outputs are copied, one SG at a time; raising fails just this SG
        sg = sg_by_name[spec.name]
        assert result.outputs is not None
        if registrar is not None:
            registrar(sg, result.outputs)
        _write_outputs(out_dir, sg.id, result.outputs)

    results = run_many(
        specs,
        on_result=on_result,
        submit_interval=submit_interval,
        max_in_flight=max_in_flight,
        context=ctx,
    )
    if results_dir:
        write_reports(results, results_dir, ctx.storage)
    report = ChunkReport(reused=reused, results=results)
    if report.failed:
        logger.error(
            '%d of %d sequencing group(s) failed: %s',
            len(report.failed),
            len(results),
            ', '.join(sg_by_name[r.name].id for r in report.failed),
        )
    elif report.exit_code == EXIT_OK:
        logger.info('all %d sequencing group(s) done (%d reused)', len(results) + len(reused), len(reused))
    return report
