"""
`nf-gatk-sv plan` (plan §5.4, D8): the first task of every run.

Resolves the config, builds the manifest from metamist, runs the entry's checks, chunks GatherSampleEvidence,
writes the content-addressed plan dir and returns `plan.json`, from which `main.nf` builds the rest of the DAG.

    <plan_dir> = gs://<tmp of workflow.dataset>/gatk_sv_nf/plans/<plan_hash>/
        config.toml   the resolved config every later process reads
        manifest.json
        gse_chunks/chunk_<i>.json   (sample_evidence) JSON lists of SG IDs

`plan_hash` covers `config.toml` and `manifest.json` only. `arGuid` is never an input, so relaunching an
unchanged plan gives the same plan dir, and downstream `-resume` cache hits survive.
"""

import hashlib
import json
import logging
import tempfile
from collections.abc import Callable, Sequence
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

import toml

from cpg_cromwell.storage import Storage, join
from gatk_sv_nf import config as cfg
from gatk_sv_nf import paths
from gatk_sv_nf.manifest import (
    Manifest,
    ManifestRequest,
    build_manifest,
    check_for_cohort_overlaps,
    combined_ped,
)
from gatk_sv_nf.metamist_client import Metamist
from gatk_sv_nf.stages import gather_sample_evidence as gse

logger = logging.getLogger(__name__)

PLAN_HASH_LENGTH = 16
EXISTS_THREADS = 32
DEFAULT_GSE_CHUNK_SIZE = 50


class PlanError(ValueError):
    """An input the run must not start with. Raised before anything is written."""


@dataclass
class Plan:
    plan_dir: str
    entry: str
    config_text: str
    manifest: Manifest
    gse_chunks: list[list[str]] = field(default_factory=list)
    # uri -> content: the PED and SG-ID files the multi_sample stages read from their cpg-flow paths
    side_files: dict[str, str] = field(default_factory=dict)

    @property
    def chunk_uris(self) -> list[str]:
        return [join(self.plan_dir, 'gse_chunks', f'chunk_{i}.json') for i in range(len(self.gse_chunks))]

    def plan_json(self, config: dict[str, Any]) -> dict[str, Any]:
        seqr = set(config['workflow'].get('write_seqr_vcf_for_datasets', []))
        return {
            'plan_dir': self.plan_dir,
            'entry': self.entry,
            'output_version': self.manifest.output_version,
            'cohort_ids': self.manifest.cohort_ids,
            'dataset_names': self.manifest.dataset_names,
            'seqr_datasets': [ds for ds in self.manifest.dataset_names if ds in seqr],
            'previous_spicy_vcf': self.manifest.previous_spicy_vcf,
            'gse_chunks': self.chunk_uris,
            'images': {key: config['images'][key] for key in ('bcftools', 'strvctvre')},
        }


def plan_hash(config_text: str, manifest_json: str) -> str:
    return hashlib.sha256((config_text + manifest_json).encode()).hexdigest()[:PLAN_HASH_LENGTH]


def _all_exist(storage: Storage, groups: Sequence[Sequence[str]]) -> list[bool]:
    """For each group of paths: do they all exist? Checked in parallel, since there may be thousands."""
    flat = sorted({p for group in groups for p in group})
    with ThreadPoolExecutor(EXISTS_THREADS) as pool:
        exists = dict(zip(flat, pool.map(storage.exists, flat), strict=True))
    return [all(exists[p] for p in group) for group in groups]


def chunk(items: Sequence[str], size: int) -> list[list[str]]:
    if size < 1:
        raise PlanError(f'--gse-chunk-size must be at least 1, got {size}')
    return [list(items[i : i + size]) for i in range(0, len(items), size)]


def _gse_todo(manifest: Manifest, config: dict[str, Any], storage: Storage, chunk_size: int) -> list[list[str]]:
    """
    The sample_evidence chunks: every SG in the manifest, as cpg-flow runs GatherSampleEvidence for every SG,
    minus those whose expected outputs all exist (D4). `GSE_CHUNK` repeats that check per SG.
    """
    sgs = list(manifest.sequencing_groups)
    if config['workflow'].get('check_expected_outputs', True):
        done = _all_exist(storage, [list(gse.expected_outputs(sg).values()) for sg in sgs])
        sgs = [sg for sg, is_done in zip(sgs, done, strict=True) if not is_done]
        logger.info('GatherSampleEvidence: %d SG(s) to run, %d already done', len(sgs), sum(done))

    if no_cram := [sg.id for sg in sgs if not sg.cram]:
        raise PlanError(f'{len(no_cram)} SG(s) have no completed cram analysis in metamist: {", ".join(no_cram)}')
    # cpg-flow asserts every registered CRAM exists; only the CRAMs this run reads are checked here
    present = _all_exist(storage, [[sg.cram] for sg in sgs if sg.cram])
    if missing := [sg.cram for sg, ok in zip(sgs, present, strict=True) if not ok]:
        raise PlanError(f'{len(missing)} CRAM(s) registered in metamist do not exist: {", ".join(missing)}')
    return chunk([sg.id for sg in sgs], chunk_size)


def _check_evidence(manifest: Manifest, storage: Storage) -> None:
    """evidence_qc: every SG in every cohort must have the evidence EvidenceQC reads (plan §13)."""
    sgs = [sg for c in manifest.cohorts for sg in manifest.cohort_sequencing_groups(c.id)]
    have = _all_exist(storage, [gse.evidence_qc_inputs(sg) for sg in sgs])
    if missing := sorted({sg.id for sg, ok in zip(sgs, have, strict=True) if not ok}):
        raise PlanError(
            f'{len(missing)} SG(s) are missing GatherSampleEvidence outputs; run the sample_evidence entry first: '
            + ', '.join(missing)
        )


def _multi_sample_files(manifest: Manifest, config: dict[str, Any], storage: Storage) -> dict[str, str]:
    """
    The files cpg-flow writes while queueing the multi-sample workflow: the combined PEDs (MakeCohortCombinedPed,
    MakeMultiCohortCombinedPed) and the active SG-ID lists. A PED that already exists is kept (D4), as cpg-flow
    skips a stage whose outputs exist; the SG-ID lists are rewritten every time, as cpg-flow does.
    """
    ov = manifest.output_version
    files: dict[str, str] = {}
    ref_ped_uri = config['references']['ped_file']
    if (ref_ped := storage.read_text(ref_ped_uri)) is None:
        raise PlanError(f'references.ped_file does not exist: {ref_ped_uri}')

    peds = {
        paths.cohort_ped(c.dataset, c.id): (manifest.cohort_sequencing_groups(c.id), c.id) for c in manifest.cohorts
    }
    peds[paths.multicohort_ped(manifest.analysis_dataset, ov)] = (list(manifest.sequencing_groups), manifest.name)
    reuse = config['workflow'].get('check_expected_outputs', True)
    for uri, (sgs, target) in peds.items():
        if reuse and storage.exists(uri):
            logger.info('keeping existing %s', uri)
            continue
        files[uri] = combined_ped(sgs, ref_ped, target)

    for ds, ids in manifest.active_sg_ids.items():
        files[paths.dataset_sgid_list(ds, ov)] = ''.join(f'{sg_id}\n' for sg_id in ids)
    all_active = sorted({sg_id for ids in manifest.active_sg_ids.values() for sg_id in ids})
    files[paths.multicohort_sgid_list(manifest.analysis_dataset, ov)] = ''.join(f'{s}\n' for s in all_active)
    return files


def resolve_billing_project(billing_project: str | None, *, dry_run: bool) -> str:
    if billing_project:
        return billing_project
    try:
        return cfg.metadata_billing_project()
    except OSError as e:
        if dry_run:
            logger.warning('no --billing-project and no metadata server (%s); using "unset" for this dry run', e)
            return 'unset'
        raise PlanError(f'no --billing-project, and the metadata server could not be read: {e}') from e


def make_plan(
    *,
    entry: str,
    run_config: dict[str, Any],
    dataset: str,
    billing_project: str,
    driver_image: str,
    storage: Storage,
    metamist: Metamist,
    gse_chunk_size: int = DEFAULT_GSE_CHUNK_SIZE,
    use_config: Callable[[str], None] | None = None,
) -> tuple[Plan, dict[str, Any]]:
    """
    Build the plan without writing it. `use_config(text)` makes the resolved config the process's cpg_utils
    config, which the path functions read (`activate_config` by default). Returns the plan and resolved config.
    """
    base = cfg.merge_layers(run_config, dataset=dataset, billing_project=billing_project, driver_image=driver_image)
    cfg.validate(base, entry)

    manifest = build_manifest(
        ManifestRequest.from_config(base, find_previous_spicy_vcf=entry == 'multi_sample'),
        metamist,
    )
    config = cfg.with_storage(base, [*manifest.dataset_names, *(c.dataset for c in manifest.cohorts)])
    config_text = cfg.dumps(config)
    (use_config or activate_config)(config_text)

    plan_dir = join(paths.plan_root(), plan_hash(config_text, manifest.to_json()))
    plan = Plan(plan_dir=plan_dir, entry=entry, config_text=config_text, manifest=manifest)

    if entry == 'sample_evidence':
        plan.gse_chunks = _gse_todo(manifest, config, storage, gse_chunk_size)
    elif entry == 'evidence_qc':
        _check_evidence(manifest, storage)
    elif entry == 'multi_sample':
        check_for_cohort_overlaps(manifest)
        plan.side_files = _multi_sample_files(manifest, config, storage)
    return plan, config


_config_dir: tempfile.TemporaryDirectory | None = None


def activate_config(config_text: str) -> None:
    """Make `config_text` the process's cpg_utils config (through a local file that lasts the process)."""
    from cpg_utils.config import set_config_paths  # noqa: PLC0415

    global _config_dir  # noqa: PLW0603
    if _config_dir is None:
        _config_dir = tempfile.TemporaryDirectory(prefix='nf-gatk-sv-plan-')
    path = Path(_config_dir.name) / f'config-{hashlib.sha256(config_text.encode()).hexdigest()[:12]}.toml'
    path.write_text(config_text)
    set_config_paths([str(path)])


def write_plan(plan: Plan, storage: Storage) -> None:
    """Write the plan dir, then the side files. Every write is idempotent: same plan, same bytes."""
    for uri, ids in zip(plan.chunk_uris, plan.gse_chunks, strict=True):
        storage.write(uri, json.dumps(ids) + '\n')
    storage.write(join(plan.plan_dir, 'manifest.json'), plan.manifest.to_json())
    storage.write(join(plan.plan_dir, 'config.toml'), plan.config_text)
    for uri, text in plan.side_files.items():
        storage.write(uri, text)
    logger.info('plan written to %s', plan.plan_dir)


def read_run_config(uri: str, storage: Storage) -> dict[str, Any]:
    if (text := storage.read_text(uri)) is None:
        raise PlanError(f'run config not found: {uri}')
    return toml.loads(text)
