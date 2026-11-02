"""
Config resolution (plan §5.5, D5).

The pipeline owns its whole config: nothing from analysis-runner's base config is available. `PLAN` builds the
resolved config from, in increasing precedence:

1. `defaults.toml` (a port of cpg-flow-gatk-sv's `config_template.toml`, minus the Hail-only keys);
2. the user's run config (`params.config`);
3. synthesised keys: `workflow.dataset`, `workflow.dataset_gcp_project`, `workflow.driver_image`, and a
   `[storage.<ds>]` section for `default` and every dataset in the manifest.

It writes the result to `<plan_dir>/config.toml`, which every later process reads through `cpg_utils.config`.
"""

import copy
import logging
import urllib.request
from collections.abc import Iterable, Mapping
from importlib import resources
from typing import Any

import toml
from cpg_utils.config import update_dict

logger = logging.getLogger(__name__)

ACCESS_LEVELS = ('test', 'standard', 'full')
ENTRIES = ('sample_evidence', 'evidence_qc', 'multi_sample')
SINGLE_SAMPLE_ENTRIES = ('sample_evidence', 'evidence_qc')

# read only by the dropped Hail stages (plan §1) or by cpg-flow itself: ignored, with a warning
DROPPED_KEYS: tuple[tuple[str, ...], ...] = (
    ('workflow', 'write_vcf'),
    ('workflow', 'gencode_gtf_file'),
    ('workflow', 'status_reporter'),
    ('references', 'liftover_38_to_37'),
    ('elasticsearch',),
    ('vcf_from_mt',),
    ('hail',),
)

# set by PLAN; a run config may only repeat workflow.dataset (and must agree with --dataset)
SYNTHESISED_WORKFLOW_KEYS = ('dataset', 'dataset_gcp_project', 'driver_image')

GSE_ONLY_JOBS = ('coverage_counts', 'pesr', 'manta', 'wham', 'scramble')

METADATA_PROJECT_URL = 'http://metadata.google.internal/computeMetadata/v1/project/project-id'


class ConfigError(ValueError):
    pass


def load_defaults() -> dict[str, Any]:
    return toml.loads(resources.files('gatk_sv_nf').joinpath('defaults.toml').read_text())


def _get(config: Mapping[str, Any], path: Iterable[str]) -> Any:
    node: Any = config
    for key in path:
        if not isinstance(node, Mapping) or key not in node:
            return None
        node = node[key]
    return node


def storage_section(dataset: str, access_level: str) -> dict[str, str]:
    """
    One `[storage.<ds>]` section, as analysis-runner writes it: `gs://cpg-<ds>-<ns>[-<category>]` with
    `ns = test` at access level test and `main` otherwise. Checked against a real analysis-runner config in P2.
    """
    ns = 'test' if access_level == 'test' else 'main'
    bucket = f'gs://cpg-{dataset}-{ns}'
    return {
        'default': bucket,
        'analysis': f'{bucket}-analysis',
        'tmp': f'{bucket}-tmp',
        'upload': f'{bucket}-upload',
        'web': f'{bucket}-web',
        'web_url': f'https://{ns}-web.populationgenomics.org.au/{dataset}',
    }


def metadata_billing_project() -> str:
    """The project of the VM this runs on (the head's billing project, plan §5.5)."""
    request = urllib.request.Request(METADATA_PROJECT_URL, headers={'Metadata-Flavor': 'Google'})
    with urllib.request.urlopen(request, timeout=5) as response:  # noqa: S310 (fixed http URL)
        return response.read().decode().strip()


def merge_layers(run_config: Mapping[str, Any], *, dataset: str, billing_project: str, driver_image: str) -> dict:
    """
    `defaults.toml` ⊕ run config ⊕ the synthesised `[workflow]` keys. `[storage.*]` is added later
    (`with_storage`), once the manifest says which datasets the run touches.
    """
    run_config = copy.deepcopy(dict(run_config))
    run_workflow = run_config.get('workflow', {})

    if (run_dataset := run_workflow.get('dataset')) and run_dataset != dataset:
        raise ConfigError(f'the run config sets workflow.dataset = {run_dataset!r}, but --dataset is {dataset!r}')
    for key in ('dataset_gcp_project', 'driver_image'):
        if key in run_workflow:
            logger.warning('ignoring workflow.%s from the run config: PLAN sets it', key)
    for path in DROPPED_KEYS:
        if _get(run_config, path) is not None:
            logger.warning('ignoring %s: only the dropped Hail stages, or cpg-flow, read it', '.'.join(path))
    if 'storage' in run_config:
        logger.warning('ignoring [storage.*] from the run config: PLAN generates it')
        del run_config['storage']

    config = update_dict(load_defaults(), run_config)
    update_dict(
        config,
        {'workflow': {'dataset': dataset, 'dataset_gcp_project': billing_project, 'driver_image': driver_image}},
    )
    return config


def with_storage(config: Mapping[str, Any], datasets: Iterable[str]) -> dict[str, Any]:
    """A copy of `config` with `[storage.default]` (the analysis dataset) and `[storage.<ds>]` for each dataset."""
    config = copy.deepcopy(dict(config))
    access_level = config['workflow']['access_level']
    analysis = config['workflow']['dataset']
    config['storage'] = {'default': storage_section(analysis, access_level)}
    for ds in sorted({analysis, *datasets}):
        config['storage'][ds] = storage_section(ds, access_level)
    return config


def validate(config: Mapping[str, Any], entry: str) -> None:
    """
    Fail in PLAN, not hours later. Every key the stages read has a default, so this checks only the keys a run
    config has to supply, and the values a run config could get wrong.
    """
    errors: list[str] = []
    if entry not in ENTRIES:
        errors.append(f'unknown entry {entry!r}; expected one of {", ".join(ENTRIES)}')

    wf = config.get('workflow', {})
    if wf.get('access_level') not in ACCESS_LEVELS:
        levels = ', '.join(ACCESS_LEVELS)
        errors.append(f'workflow.access_level must be one of {levels}, got {wf.get("access_level")!r}')
    for key in SYNTHESISED_WORKFLOW_KEYS:
        if not wf.get(key):
            errors.append(f'workflow.{key} is not set')

    cohorts = wf.get('input_cohorts')
    datasets = wf.get('input_datasets')
    if cohorts is not None and (not isinstance(cohorts, list) or not cohorts):
        errors.append('workflow.input_cohorts must be a non-empty list')
    if datasets is not None and (not isinstance(datasets, list) or not datasets):
        errors.append('workflow.input_datasets must be a non-empty list')
    if cohorts and datasets:
        errors.append('set one of workflow.input_cohorts and workflow.input_datasets, not both')
    elif not cohorts and not datasets:
        errors.append('no input: set workflow.input_cohorts (or, for sample_evidence only, workflow.input_datasets)')
    elif datasets and entry != 'sample_evidence':
        errors.append(f'workflow.input_datasets is only accepted by the sample_evidence entry; {entry} needs cohorts')
    if wf.get('only_sgs') and wf.get('skip_sgs'):
        errors.append('Cannot specify both only_sgs and skip_sgs in config')

    if entry in SINGLE_SAMPLE_ENTRIES and wf.get('permit_inactive_cohorts'):
        # cpg-flow-gatk-sv singlesample_workflow.cli_main
        errors.append('Initial variant calling should not tolerate inactive SGs (workflow.permit_inactive_cohorts)')

    if wf.get('populate_assays'):
        errors.append('workflow.populate_assays is not supported (it changes output_version to an assay hash)')

    if not isinstance(wf.get('write_seqr_vcf_for_datasets', []), list):
        errors.append('workflow.write_seqr_vcf_for_datasets must be a list')

    only_jobs = _get(wf, ['GatherSampleEvidence', 'only_jobs'])
    if only_jobs is not None and (not isinstance(only_jobs, list) or set(only_jobs) - set(GSE_ONLY_JOBS)):
        jobs = ', '.join(GSE_ONLY_JOBS)
        errors.append(f'workflow.GatherSampleEvidence.only_jobs must be a list drawn from {jobs}')

    if errors:
        raise ConfigError('invalid config:\n  ' + '\n  '.join(errors))


def dumps(config: Mapping[str, Any]) -> str:
    return toml.dumps(dict(config))
