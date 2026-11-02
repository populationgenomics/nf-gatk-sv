"""
Metamist analysis registration (plan §7).

Port of `cpg_flow.status.complete_analysis_job` plus the bits of `cpg_flow.stage.Stage` and
`cpg_flow.metamist.Metamist` that decide the project name and meta.
"""

import logging
from collections.abc import Callable, Mapping, Sequence
from typing import Any

from cpg_utils import to_path
from cpg_utils.config import config_retrieve

logger = logging.getLogger(__name__)

# (project, output, analysis_type, sequencing_group_ids, cohort_ids, meta) -> analysis id
CreateAnalysis = Callable[[str, str, str, list[str], list[str], dict[str, Any]], int | None]
SizeOf = Callable[[str], int]


def metamist_project(dataset: str) -> str:
    """
    Effective project name: `-test` appended at access level test.

    cpg_flow appends it twice over (Stage when 'test' isn't in the name, then
    Metamist.get_metamist_proj when the name doesn't end in '-test'); the net rule is the latter.
    """
    if config_retrieve(['workflow', 'access_level']) == 'test' and not dataset.endswith('-test'):
        return f'{dataset}-test'
    return dataset


def stage_job_attrs(stage: str, target_attrs: Mapping[str, str]) -> dict[str, str]:
    """Port of `cpg_flow.stage.Stage.get_job_attrs`: the base of every analysis meta."""
    attrs = {'stage': stage}
    if sequencing_type := config_retrieve(['workflow', 'sequencing_type'], None):
        attrs['sequencing_type'] = sequencing_type
    return attrs | dict(target_attrs)


def _metamist_create(
    project: str,
    output: str,
    analysis_type: str,
    sequencing_group_ids: list[str],
    cohort_ids: list[str],
    meta: dict[str, Any],
) -> int | None:
    from metamist.apis import AnalysisApi  # noqa: PLC0415
    from metamist.models import Analysis, AnalysisStatus  # noqa: PLC0415

    analysis = Analysis(
        type=analysis_type,
        status=AnalysisStatus('completed'),
        output=output,
        cohort_ids=cohort_ids,
        sequencing_group_ids=sequencing_group_ids,
        meta=meta,
    )
    return AnalysisApi().create_analysis(project=project, analysis=analysis)


def _size_of(output: str) -> int:
    return to_path(output).stat().st_size


def register_analysis(
    *,
    output: str,
    analysis_type: str,
    dataset: str,
    meta: Mapping[str, Any],
    sequencing_group_ids: Sequence[str] = (),
    cohort_ids: Sequence[str] = (),
    create: CreateAnalysis = _metamist_create,
    size_of: SizeOf = _size_of,
) -> int:
    """
    Create one `completed` analysis. `dataset` is the target's dataset (unsuffixed).

    Raises if the output doesn't exist (`complete_analysis_job` with tolerate_missing=False), or if
    metamist doesn't return an ID.
    """
    meta = dict(meta)
    meta.pop('sequencing_groups', None)
    sg_ids = list(sequencing_group_ids)
    if 'remove_sgids' in meta and to_path(meta['remove_sgids']).exists():
        exclusions = set(to_path(meta['remove_sgids']).read_text().splitlines())
        logger.info('removing %d sequencing groups from analysis: %s', len(exclusions), ', '.join(sorted(exclusions)))
        sg_ids = [sg for sg in sg_ids if sg not in exclusions]

    try:
        meta['size'] = size_of(output)
    except FileNotFoundError as e:
        raise ValueError(f"Output {output} doesn't exist") from e

    project = metamist_project(dataset)
    analysis_id = create(project, output, analysis_type, sg_ids, list(cohort_ids), meta)
    if analysis_id is None:
        raise ConnectionError(f'Creation of Analysis failed (type={analysis_type}, output={output}) in {project}')
    logger.info('Created Analysis(id=%s, type=%s, output=%s) in %s', analysis_id, analysis_type, output, project)
    return analysis_id
