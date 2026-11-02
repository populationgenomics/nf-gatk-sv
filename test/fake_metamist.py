"""
A fake `metamist.graphql.query`, answering from canned data. Dependency-free, so the offline P1
(`parity/p1_offline.py`) can patch it into cpg-flow (in cpg-flow-gatk-sv's env) as well as into nf-gatk-sv.

It recognises each query by its fields, not its operation name, so it answers cpg-flow's query texts and
nf-gatk-sv's (`gatk_sv_nf.metamist_client`) alike. Fixture shape:

    {"cohorts": {"COH1": {"name": "...", "status": "active", "dataset": "ds", "sequencing_groups": ["CPG1", ...]}},
     "projects": {"ds-test": {                          # the metamist project name, -test suffix included
         "sequencing_groups": [<raw SG entry>, ...],    # as the GraphQL API returns them
         "inactive": ["CPG9"],                          # SG IDs activeOnly leaves out
         "analyses": {"cram": [...], "sv": [...]},      # completed, active analyses, in query order
         "pedigree": [{"family_id", "individual_id", "paternal_id", "maternal_id", "sex", "affected"}]}}}

A cohort's SGs are looked up across every project's `sequencing_groups`.
"""

from collections.abc import Mapping
from typing import Any


def _text(document: Any) -> str:
    if isinstance(document, str):
        return document
    loc = getattr(document, 'loc', None)  # a gql DocumentNode
    if loc is not None and getattr(loc, 'source', None) is not None:
        return loc.source.body
    from graphql import print_ast

    return print_ast(document)


def _squash(text: str) -> str:
    return ''.join(text.split())


class FakeMetamist:
    def __init__(self, fixture: Mapping[str, Any]) -> None:
        self.fixture = fixture
        self.calls: list[tuple[str, dict[str, Any]]] = []

    def _project(self, name: str) -> Mapping[str, Any]:
        if name not in self.fixture['projects']:
            raise KeyError(f'fake metamist: no project {name!r}')
        return self.fixture['projects'][name]

    def _sg(self, sg_id: str) -> dict[str, Any]:
        for project in self.fixture['projects'].values():
            for sg in project.get('sequencing_groups', []):
                if sg['id'] == sg_id:
                    return sg
        raise KeyError(f'fake metamist: no SG {sg_id!r}')

    def query(self, document: Any, variables: Mapping[str, Any] | None = None, **_: Any) -> dict[str, Any]:
        q = _squash(_text(document))
        v = dict(variables or {})
        if 'cohorts(id:{in_:' in q:
            self.calls.append(('cohort_statuses', v))
            cohorts = self.fixture['cohorts']
            return {'cohorts': [{'id': c, 'status': cohorts[c]['status']} for c in v['cohorts'] if c in cohorts]}
        if 'cohorts(id:{eq:' in q:
            self.calls.append(('cohort', v))
            c = self.fixture['cohorts'].get(v['cohort_id'])
            if c is None:
                return {'cohorts': []}
            return {
                'cohorts': [
                    {
                        'name': c['name'],
                        'status': c['status'],
                        'project': {'dataset': c.get('dataset')},
                        'sequencingGroups': [self._sg(s) for s in c['sequencing_groups']],
                    }
                ]
            }
        if 'activeOnly' in q:
            self.calls.append(('active', v))
            project = self._project(v['metamist_proj'])
            inactive = set(project.get('inactive', []))
            ids = [sg['id'] for sg in project['sequencing_groups']]
            return {
                'project': {'sequencingGroups': [{'id': i} for i in ids if i in v['only_sgs'] and i not in inactive]}
            }
        if 'pedigree(' in q:
            self.calls.append(('pedigree', v))
            return {'project': {'pedigree': self._project(v['metamist_proj']).get('pedigree', [])}}
        if 'timestampCompleted' in q:
            self.calls.append(('sv', v))
            return {'project': {'analyses': self._project(v['dataset']).get('analyses', {}).get('sv', [])}}
        if 'analyses(active:' in q:
            self.calls.append(('analyses', v))
            analyses = self._project(v['metamist_proj']).get('analyses', {}).get(v['analysis_type'], [])
            return {'project': {'analyses': analyses}}
        if 'nin:$skip_sgs' in q:
            self.calls.append(('project_sgs', v))
            sgs = [
                sg
                for sg in self._project(v['metamist_proj'])['sequencing_groups']
                if (not v['only_sgs'] or sg['id'] in v['only_sgs'])
                and sg['id'] not in v['skip_sgs']
                and sg['type'] == v['sequencing_type']
            ]
            return {'project': {'sequencingGroups': sgs}}
        raise NotImplementedError(f'fake metamist: unrecognised query {q[:120]}')


def sg_entry(
    sg_id: str,
    project: str,
    *,
    external_id: str,
    participant_id: str | None,
    reported_sex: int | None = None,
    meta: dict | None = None,
    participant_meta: dict | None = None,
    phenotypes: dict | None = None,
    sequencing_type: str = 'genome',
) -> dict[str, Any]:
    """A raw SG entry, as the GraphQL API returns it."""
    return {
        'id': sg_id,
        'meta': meta or {},
        'platform': 'illumina',
        'technology': 'short-read',
        'type': sequencing_type,
        'sample': {
            'project': {'name': project},
            'externalId': external_id,
            'participant': {
                'id': 1,
                'externalId': participant_id,
                'phenotypes': phenotypes or {},
                'reportedSex': reported_sex,
                'meta': participant_meta or {},
            },
        },
        'assays': [],
    }


def cram_analysis(analysis_id: int, sg_id: str, output: str) -> dict[str, Any]:
    return {
        'id': analysis_id,
        'type': 'cram',
        'meta': {},
        'output': output,
        'status': 'COMPLETED',
        'sequencingGroups': [{'id': sg_id}],
    }


def fixture(ns: str = 'test') -> dict[str, Any]:
    """
    Two cohorts over two datasets (plus a third dataset only cohort B touches), covering the manifest rules:
    cohort order (B listed first in config, sorted to second), dataset order of first appearance, an SG in both
    cohorts' datasets' order, parents in the same dataset, a parent in another dataset (not resolved), no PED
    entry, reportedSex only, an unclean family ID, two CRAMs for one SG (last wins), an SG with no CRAM, an
    inactive SG, and meta merged from SG and participant.
    """
    sfx = '-test' if ns == 'test' else ''

    def p(ds: str) -> str:
        return f'{ds}{sfx}'

    def cram(ds: str, sg: str) -> str:
        return f'gs://cpg-{ds}-{ns}/cram/{sg}.cram'

    return {
        'cohorts': {
            'COH2': {
                'name': 'second',
                'status': 'active',
                'dataset': 'ds-one',
                'sequencing_groups': ['CPG30', 'CPG40'],
            },
            'COH1': {
                'name': 'first',
                'status': 'active',
                'dataset': None,  # falls back to the analysis dataset
                'sequencing_groups': ['CPG20', 'CPG10', 'CPG11', 'CPG12'],
            },
        },
        'projects': {
            p('ds-one'): {
                'sequencing_groups': [
                    sg_entry('CPG10', p('ds-one'), external_id='S10', participant_id='P10', meta={'a': 1}),
                    sg_entry('CPG11', p('ds-one'), external_id='S11', participant_id='P11', reported_sex=2),
                    sg_entry('CPG12', p('ds-one'), external_id='S12', participant_id='P12', reported_sex=1),
                    sg_entry(
                        'CPG30',
                        p('ds-one'),
                        external_id='S30',
                        participant_id=None,  # falls back to the external ID
                        meta={'x': {'y': 1}},
                        participant_meta={'x': {'z': 2}},
                        phenotypes={'HP:1': 'yes'},
                    ),
                ],
                'inactive': ['CPG11'],
                'analyses': {
                    'cram': [
                        cram_analysis(1, 'CPG10', cram('ds-one', 'CPG10-old')),
                        cram_analysis(2, 'CPG10', cram('ds-one', 'CPG10')),
                        cram_analysis(3, 'CPG11', cram('ds-one', 'CPG11')),
                        cram_analysis(4, 'CPG12', cram('ds-one', 'CPG12')),
                        cram_analysis(5, 'CPG30', cram('ds-one', 'CPG30')),
                        {**cram_analysis(6, 'CPG99', 'gs://x'), 'sequencingGroups': []},
                    ],
                },
                'pedigree': [
                    # child with both parents in this dataset; family ID needs cleaning
                    {
                        'family_id': 'FAM-1',
                        'individual_id': 'P10',
                        'paternal_id': 'P12',
                        'maternal_id': 'P11',
                        'sex': 1,
                        'affected': 2,
                    },
                    {
                        'family_id': 'FAM-1',
                        'individual_id': 'P11',
                        'paternal_id': None,
                        'maternal_id': None,
                        'sex': 2,
                        'affected': 1,
                    },
                    {
                        'family_id': 'FAM-1',
                        'individual_id': 'P12',
                        'paternal_id': '0',
                        'maternal_id': '0',
                        'sex': 'male',
                        'affected': None,
                    },
                ],
            },
            p('ds-two'): {
                'sequencing_groups': [
                    sg_entry('CPG20', p('ds-two'), external_id='S20', participant_id='P20', reported_sex=1),
                    # mother is P10, but in another dataset: not resolved
                    sg_entry('CPG40', p('ds-two'), external_id='S40', participant_id='P40'),
                ],
                'analyses': {'cram': [cram_analysis(7, 'CPG20', cram('ds-two', 'CPG20'))]},
                'pedigree': [
                    {
                        'family_id': 'FAM2',
                        'individual_id': 'P40',
                        'paternal_id': None,
                        'maternal_id': 'P10',
                        'sex': 0,
                        'affected': 0,
                    },
                ],
            },
            p('analysis-ds'): {
                'sequencing_groups': [],
                'analyses': {
                    'sv': [
                        {'output': 'gs://a/old/fresh_ids.vcf.bgz', 'timestampCompleted': '2024-01-01T00:00:00'},
                        {'output': 'gs://a/new/fresh_ids.vcf.bgz', 'timestampCompleted': '2025-06-01T00:00:00'},
                        {'output': 'gs://a/newer/other.vcf.bgz', 'timestampCompleted': '2026-01-01T00:00:00'},
                        {'output': None, 'timestampCompleted': '2027-01-01T00:00:00'},
                    ]
                },
            },
        },
    }
