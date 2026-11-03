"""
The manifest (plan §5.4): everything `nf-gatk-sv plan` learns from metamist, written once to
`<plan_dir>/manifest.json` and read by every later process. It replaces `cpg_flow.inputs.get_multicohort` and the
cpg-flow target model, and is checked against them by P1 (`parity/p1_offline.py`).

`build_manifest` ports, with the same ordering rules:
- `cpg_flow.inputs.create_multicohort`: cohorts sorted by ID; datasets in order of first appearance; SGs in
  insertion order per dataset; SG meta merged from SG and participant meta, plus `phenotypes`;
- `_populate_analysis` (the CRAM: the last completed, active `cram` analysis per SG, in query order);
- `_populate_pedigree` (parents resolved by participant ID within the SG's dataset);
- `Workflow.output_version` and `MultiCohort.name`;
- `cpg_flow_gatk_sv.utils`: `get_active_sg_ids` and `query_for_spicy_vcf`.

`manifest.json`:

    {"multicohort": {"name", "analysis_dataset", "cohorts": [{"id", "name", "dataset", "sequencing_groups"}]},
     "datasets": [{"name", "active_sg_ids"}],          # active IDs in the order metamist returned them
     "sequencing_groups": [{"id", "dataset", "cram", "active", "participant_id", "external_id", "meta",
                            "pedigree": {"family_id", "father_id", "mother_id", "sex", "phenotype"}}],
     "output_version", "previous_spicy_vcf"}

`sequencing_groups` is in `MultiCohort.get_sequencing_groups()` order: by dataset, then insertion order.
"""

import hashlib
import itertools
import json
import logging
import re
import unicodedata
from collections.abc import Iterable, Mapping, Sequence
from dataclasses import asdict, dataclass, field
from typing import Any

from cpg_utils.config import update_dict

from gatk_sv_nf.metamist_client import Metamist, MetamistError

logger = logging.getLogger(__name__)

PED_FAMILY_ID_REGEX = re.compile(r'(^[A-Za-z0-9_]+$)')


# ---- the model ------------------------------------------------------------------------------------------------


@dataclass(frozen=True)
class Pedigree:
    """One PED row's fields, as cpg_flow `PedigreeInfo.get_ped_dict` gives them (family ID not yet cleaned)."""

    family_id: str
    father_id: str = '0'
    mother_id: str = '0'
    sex: str = '0'
    phenotype: str = '0'


@dataclass(frozen=True)
class SequencingGroup:
    id: str
    dataset: str
    cram: str | None
    active: bool = True
    participant_id: str | None = None  # cpg_flow `sg._participant_id`: the raw participant external ID
    external_id: str | None = None  # cpg_flow `sg._external_id`
    meta: dict[str, Any] = field(default_factory=dict, compare=False, hash=False)
    pedigree: Pedigree | None = None

    @classmethod
    def from_dict(cls, data: Mapping[str, Any]) -> SequencingGroup:
        return cls(
            id=data['id'],
            dataset=data['dataset'],
            cram=data.get('cram'),
            active=data.get('active', True),
            participant_id=data.get('participant_id'),
            external_id=data.get('external_id'),
            meta=data.get('meta') or {},
            pedigree=Pedigree(**data['pedigree']) if data.get('pedigree') else None,
        )

    @property
    def effective_participant_id(self) -> str:
        """cpg_flow `sg.participant_id`: the participant ID, else the sample external ID, else the SG ID."""
        return self.participant_id or self.external_id or self.id

    def job_attrs(self) -> dict[str, str]:
        """Port of cpg_flow `SequencingGroup.get_job_attrs` (feeds the analysis meta, plan §7)."""
        attrs = {'dataset': self.dataset, 'sequencing_group': self.id}
        if participant_id := self.participant_id or self.external_id:
            attrs['participant_id'] = participant_id
        return attrs

    def ped_fields(self) -> list[str]:
        """`[family, individual, father, mother, sex, phenotype]`, as `get_ped_dict()` gives them."""
        ped = self.pedigree or Pedigree(family_id=self.effective_participant_id)
        return [ped.family_id, self.id, ped.father_id, ped.mother_id, ped.sex, ped.phenotype]


@dataclass(frozen=True)
class Cohort:
    id: str
    name: str
    dataset: str  # the cohort's analysis dataset (cpg_flow `cohort.dataset`)
    sequencing_group_ids: tuple[str, ...]


@dataclass(frozen=True)
class Manifest:
    name: str  # cpg_flow `MultiCohort.name`
    analysis_dataset: str
    cohorts: tuple[Cohort, ...]
    active_sg_ids: dict[str, tuple[str, ...]]  # dataset -> active SG IDs, in dataset order, metamist order
    sequencing_groups: tuple[SequencingGroup, ...]
    output_version: str
    previous_spicy_vcf: str | None = None

    @property
    def dataset_names(self) -> list[str]:
        return list(self.active_sg_ids)

    @property
    def cohort_ids(self) -> list[str]:
        return [c.id for c in self.cohorts]

    def sg_by_id(self) -> dict[str, SequencingGroup]:
        return {sg.id: sg for sg in self.sequencing_groups}

    def cohort(self, cohort_id: str) -> Cohort:
        for cohort in self.cohorts:
            if cohort.id == cohort_id:
                return cohort
        raise KeyError(f'cohort {cohort_id} is not in the manifest')

    def cohort_sequencing_groups(self, cohort_id: str) -> list[SequencingGroup]:
        by_id = self.sg_by_id()
        return [by_id[sg_id] for sg_id in self.cohort(cohort_id).sequencing_group_ids]

    def dataset_sequencing_groups(self, dataset: str) -> list[SequencingGroup]:
        return [sg for sg in self.sequencing_groups if sg.dataset == dataset]

    def to_dict(self) -> dict[str, Any]:
        return {
            'multicohort': {
                'name': self.name,
                'analysis_dataset': self.analysis_dataset,
                'cohorts': [
                    {
                        'id': c.id,
                        'name': c.name,
                        'dataset': c.dataset,
                        'sequencing_groups': list(c.sequencing_group_ids),
                    }
                    for c in self.cohorts
                ],
            },
            'datasets': [{'name': ds, 'active_sg_ids': list(ids)} for ds, ids in self.active_sg_ids.items()],
            'sequencing_groups': [
                {
                    'id': sg.id,
                    'dataset': sg.dataset,
                    'cram': sg.cram,
                    'active': sg.active,
                    'participant_id': sg.participant_id,
                    'external_id': sg.external_id,
                    'meta': sg.meta,
                    'pedigree': asdict(sg.pedigree) if sg.pedigree else None,
                }
                for sg in self.sequencing_groups
            ],
            'output_version': self.output_version,
            'previous_spicy_vcf': self.previous_spicy_vcf,
        }

    def to_json(self) -> str:
        """Deterministic: the plan hash covers this text (plan §5.4)."""
        return json.dumps(self.to_dict(), indent=2, sort_keys=True) + '\n'

    @classmethod
    def from_dict(cls, data: Mapping[str, Any]) -> Manifest:
        mc = data['multicohort']
        return cls(
            name=mc['name'],
            analysis_dataset=mc['analysis_dataset'],
            cohorts=tuple(
                Cohort(
                    id=c['id'],
                    name=c['name'],
                    dataset=c['dataset'],
                    sequencing_group_ids=tuple(c['sequencing_groups']),
                )
                for c in mc['cohorts']
            ),
            active_sg_ids={d['name']: tuple(d['active_sg_ids']) for d in data['datasets']},
            sequencing_groups=tuple(SequencingGroup.from_dict(sg) for sg in data['sequencing_groups']),
            output_version=data['output_version'],
            previous_spicy_vcf=data.get('previous_spicy_vcf'),
        )

    @classmethod
    def from_json(cls, text: str) -> Manifest:
        return cls.from_dict(json.loads(text))


def load_sequencing_groups(text: str) -> dict[str, SequencingGroup]:
    """`{sg_id: SequencingGroup}` from manifest.json text, in manifest order."""
    data = json.loads(text)
    sgs = [SequencingGroup.from_dict(d) for d in data['sequencing_groups']]
    by_id = {sg.id: sg for sg in sgs}
    if len(by_id) != len(sgs):
        raise ValueError('manifest lists a sequencing group more than once')
    return by_id


# ---- ports of cpg-flow helpers --------------------------------------------------------------------------------


def parse_sex(value: str | int | None) -> int:
    """Port of `cpg_flow.targets.Sex.parse`, as the PED value (0, 1, 2)."""
    if value:
        sex = value.lower() if isinstance(value, str) else value
        if sex in {'m', 'male', '1', 1}:
            return 1
        if sex in {'f', 'female', '2', 2}:
            return 2
        if sex in {'u', 'unknown', '0', 0}:
            return 0
        raise ValueError(f'Unrecognised sex value {value}')
    return 0


def slugify(line: str) -> str:
    """Port of `cpg_flow.utils.slugify`."""
    line = unicodedata.normalize('NFKD', line).encode('ascii', 'ignore').decode()
    line = line.strip().lower()
    return re.sub(r'[\s.]+', '-', line)


def hash_from_list_of_strings(strings: Sequence[str], hash_length: int = 10, suffix: str | None = None) -> str:
    """Port of `cpg_flow.utils.hash_from_list_of_strings`."""
    full = f'{hashlib.sha256(" ".join(strings).encode()).hexdigest()[:hash_length]}_{len(strings)}'
    return f'{full}_{suffix}' if suffix else full


def sg_hash(sg_ids: Iterable[str]) -> str:
    """Port of `Target.set_sg_hash`: the default `output_version`."""
    ids = sorted(sg_ids)
    return f'{hashlib.sha256("".join(ids).encode()).hexdigest()[:38]}_{len(ids)}'


def metamist_project(dataset: str, access_level: str) -> str:
    """`Metamist.get_metamist_proj`: `-test` appended at access level test."""
    return f'{dataset}-test' if access_level == 'test' and not dataset.endswith('-test') else dataset


def clean_ped_family_id(family_id: str) -> str:
    """Port of `cpg_flow_gatk_sv.utils.clean_ped_family_id`."""
    return family_id if PED_FAMILY_ID_REGEX.match(family_id) else re.sub(r'[^A-Za-z0-9_]', '_', family_id)


def combined_ped(sgs: Iterable[SequencingGroup], ref_panel_ped: str, target: str) -> str:
    """
    Port of `cpg_flow_gatk_sv.utils.make_combined_ped`: one tab-separated row per SG, with the family ID
    cleaned, then the reference panel PED (`references.ped_file`) appended as it is.
    """
    lines = []
    for sg in sgs:
        family, *rest = sg.ped_fields()
        lines.append('\t'.join([clean_ped_family_id(family), *rest]) + '\n')
    if not lines:
        raise ValueError(f'No pedigree data found for {target}')
    return ''.join(lines) + ref_panel_ped


def check_for_cohort_overlaps(manifest: Manifest) -> None:
    """Port of `cpg_flow_gatk_sv.utils.check_for_cohort_overlaps`: GATK-SV can't take overlapping cohorts."""
    sgs_per_cohort = {c.id: set(c.sequencing_group_ids) for c in manifest.cohorts}
    errors = [
        f'Overlapping cohorts {id1} and {id2} have overlapping SGs: {overlap}'
        for id1, id2 in itertools.combinations(sgs_per_cohort, 2)
        if (overlap := sgs_per_cohort[id1] & sgs_per_cohort[id2])
    ]
    if errors:
        raise ValueError('\n'.join(errors))


# ---- building it from metamist --------------------------------------------------------------------------------


@dataclass(frozen=True)
class ManifestRequest:
    """What `build_manifest` needs from the resolved config."""

    analysis_dataset: str
    access_level: str
    input_cohorts: tuple[str, ...] = ()
    input_datasets: tuple[str, ...] = ()
    only_sgs: tuple[str, ...] = ()
    skip_sgs: tuple[str, ...] = ()
    sequencing_type: str = 'genome'
    permit_inactive_cohorts: bool = False
    read_pedigree: bool = True
    output_version: str | None = None  # workflow.output_version
    find_previous_spicy_vcf: bool = False

    @classmethod
    def from_config(cls, config: Mapping[str, Any], *, find_previous_spicy_vcf: bool = False) -> ManifestRequest:
        wf = config['workflow']
        return cls(
            analysis_dataset=wf['dataset'],
            access_level=wf['access_level'],
            input_cohorts=tuple(wf.get('input_cohorts') or ()),
            input_datasets=tuple(wf.get('input_datasets') or ()),
            only_sgs=tuple(wf.get('only_sgs') or ()),
            skip_sgs=tuple(wf.get('skip_sgs') or ()),
            sequencing_type=wf.get('sequencing_type', 'genome'),
            permit_inactive_cohorts=bool(wf.get('permit_inactive_cohorts', False)),
            read_pedigree=bool(wf.get('read_pedigree', True)),
            output_version=wf.get('output_version'),
            find_previous_spicy_vcf=find_previous_spicy_vcf,
        )


@dataclass
class _SgDraft:
    id: str
    dataset: str
    external_id: str
    participant_id: str | None
    meta: dict[str, Any]
    sex: int = 0
    cram: str | None = None
    pedigree: Pedigree | None = None

    @property
    def effective_participant_id(self) -> str:
        return self.participant_id or self.external_id or self.id


def _sg_draft(entry: Mapping[str, Any], dataset: str) -> _SgDraft:
    """Port of `cpg_flow.inputs.add_sg_to_dataset` (the parts that reach the manifest)."""
    participant = entry['sample']['participant']
    meta = dict(entry.get('meta') or {})
    update_dict(meta, participant.get('meta') or {})
    update_dict(meta, {'phenotypes': participant.get('phenotypes') or {}})
    draft = _SgDraft(
        id=str(entry['id']),
        dataset=dataset,
        external_id=str(entry['sample']['externalId']),
        participant_id=participant.get('externalId'),
        meta=meta,
    )
    if reported_sex := participant.get('reportedSex'):
        draft.sex = parse_sex(reported_sex)
    return draft


def _cram_by_sg(analyses: Iterable[Mapping[str, Any]]) -> dict[str, str | None]:
    """`Metamist.get_analyses_by_sgid`: one analysis per SG, the last one in query order winning."""
    by_sg: dict[str, str | None] = {}
    for analysis in analyses:
        sg_ids = {s['id'] for s in analysis['sequencingGroups']}
        if not sg_ids:
            logger.warning('Analysis has no sequencing group ids. %s', analysis)
            continue
        if len(sg_ids) != 1:
            raise MetamistError(f'cram analysis {analysis.get("id")} is linked to {len(sg_ids)} sequencing groups')
        by_sg[sg_ids.pop()] = analysis.get('output')
    return by_sg


def _pedigrees(sgs: Sequence[_SgDraft], entries: Iterable[Mapping[str, Any]]) -> list[str]:
    """Port of `cpg_flow.inputs._populate_pedigree` for one dataset. Returns the SGs with no PED entry."""
    by_participant = {sg.effective_participant_id: sg for sg in sgs}
    ped_by_participant = {str(e['individual_id']): e for e in entries}
    missing = []
    for sg in sgs:
        if (ped := ped_by_participant.get(sg.effective_participant_id)) is None:
            missing.append(sg.id)
            continue
        mother = by_participant.get(str(ped['maternal_id']))
        father = by_participant.get(str(ped['paternal_id']))
        sg.sex = parse_sex(str(ped['sex']))
        sg.pedigree = Pedigree(
            family_id=ped['family_id'] or sg.effective_participant_id,
            father_id=father.id if father else '0',
            mother_id=mother.id if mother else '0',
            sex=str(sg.sex),
            phenotype=str(ped['affected'] or '0'),
        )
    return missing


def _check_cohorts_active(cohort_ids: Sequence[str], mm: Metamist, permit_inactive: bool) -> None:
    """Port of `cpg_flow.metamist.check_for_inactive_cohorts`."""
    inactive = [cid for cid, status in mm.cohort_statuses(cohort_ids).items() if status != 'active']
    if not inactive:
        return
    if permit_inactive:
        logger.warning('Inactive Cohorts detected: %s. Continuing anyway (workflow.permit_inactive_cohorts)', inactive)
    else:
        raise MetamistError(
            'Some Cohorts in the input list are inactive, only active cohorts are allowed.\n'
            f'Inactive Cohorts: {inactive}'
        )


def previous_spicy_vcf(mm: Metamist, dataset: str, access_level: str) -> str | None:
    """
    Port of `cpg_flow_gatk_sv.utils.query_for_spicy_vcf`: the latest completed, active `sv` analysis whose output
    ends in `fresh_ids.vcf.bgz`. Note the `-test` rule here is "`test` not in the name", unlike `metamist_project`.
    """
    project = f'{dataset}-test' if access_level == 'test' and 'test' not in dataset else dataset
    by_date = {
        a['timestampCompleted']: a['output']
        for a in mm.sv_analyses(project)
        if a['output'] and a['output'].endswith('fresh_ids.vcf.bgz')
    }
    return by_date[sorted(by_date)[-1]] if by_date else None


def build_manifest(request: ManifestRequest, mm: Metamist) -> Manifest:
    """Query metamist and build the manifest. Raises `MetamistError`/`ValueError` on inputs cpg-flow refuses."""
    datasets: dict[str, dict[str, _SgDraft]] = {}  # creation order, then SG insertion order
    cohorts: list[Cohort] = []

    def add(entry: Mapping[str, Any], dataset: str) -> _SgDraft:
        sgs = datasets.setdefault(dataset, {})
        if (sg_id := str(entry['id'])) not in sgs:  # the first sighting wins, as Dataset.add_sequencing_group
            sgs[sg_id] = _sg_draft(entry, dataset)
        return sgs[sg_id]

    if request.input_cohorts:
        cohort_ids = sorted(set(request.input_cohorts))
        if len(cohort_ids) != len(request.input_cohorts):
            logger.warning('Removed non-unique cohort IDs from workflow.input_cohorts')
        _check_cohorts_active(cohort_ids, mm, request.permit_inactive_cohorts)
        for cohort_id in cohort_ids:
            data = mm.cohort(cohort_id)
            if not data['sequencing_groups']:
                raise MetamistError(f'Cohort {cohort_id} has no sequencing groups')
            members: dict[str, None] = {}
            for entry in data['sequencing_groups']:
                sg = add(entry, entry['sample']['project']['name'].removesuffix('-test'))
                members[sg.id] = None
            cohorts.append(
                Cohort(
                    id=cohort_id,
                    name=data['name'] or request.analysis_dataset,  # as cpg_flow Cohort()
                    dataset=data['dataset'] or request.analysis_dataset,
                    sequencing_group_ids=tuple(members),
                )
            )
        name = hash_from_list_of_strings(sorted(request.input_cohorts), suffix='cohorts')
    else:
        # sample_evidence only (config.validate): every SG of the sequencing type in each dataset
        for input_dataset in request.input_datasets:
            dataset = input_dataset.removesuffix('-test')
            entries = mm.project_sequencing_groups(
                metamist_project(dataset, request.access_level),
                only_sgs=list(request.only_sgs),
                skip_sgs=list(request.skip_sgs),
                sequencing_type=request.sequencing_type,
            )
            for entry in entries:
                add(entry, dataset)
        name = request.analysis_dataset

    if request.input_cohorts and (request.only_sgs or request.skip_sgs):
        logger.warning('workflow.only_sgs / skip_sgs are ignored with input_cohorts, as cpg-flow ignores them')

    active: dict[str, tuple[str, ...]] = {}
    for dataset, sgs in datasets.items():
        project = metamist_project(dataset, request.access_level)
        crams = _cram_by_sg(mm.completed_analyses(project, 'cram'))
        for sg in sgs.values():
            sg.cram = crams.get(sg.id) or None
        if request.read_pedigree and (missing := _pedigrees(list(sgs.values()), mm.pedigree(project))):
            logger.warning('No pedigree data found for %d/%d sequencing groups in %s', len(missing), len(sgs), dataset)
        # cpg-flow-gatk-sv queries the unsuffixed name here; this uses the -test project at access level test
        active[dataset] = tuple(mm.active_sg_ids(project, list(sgs)))

    all_sgs = [sg for sgs in datasets.values() for sg in sgs.values()]
    output_version = slugify(request.output_version) if request.output_version else sg_hash(sg.id for sg in all_sgs)
    spicy = (
        previous_spicy_vcf(mm, request.analysis_dataset, request.access_level)
        if request.find_previous_spicy_vcf
        else None
    )
    return Manifest(
        name=name,
        analysis_dataset=request.analysis_dataset,
        cohorts=tuple(cohorts),
        active_sg_ids=active,
        sequencing_groups=tuple(
            SequencingGroup(
                id=sg.id,
                dataset=sg.dataset,
                cram=sg.cram,
                active=sg.id in active[sg.dataset],
                participant_id=sg.participant_id,
                external_id=sg.external_id,
                meta=sg.meta,
                pedigree=sg.pedigree or Pedigree(family_id=sg.effective_participant_id, sex=str(sg.sex)),
            )
            for sg in all_sgs
        ),
        output_version=output_version,
        previous_spicy_vcf=spicy,
    )
