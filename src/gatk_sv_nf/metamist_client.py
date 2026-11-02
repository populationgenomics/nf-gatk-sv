"""
The metamist GraphQL queries `nf-gatk-sv plan` makes (plan §5.4).

The query texts are cpg-flow's (`cpg_flow.metamist`, `cpg_flow_gatk_sv.utils`), so the manifest is built from the
same data cpg-flow sees. Everything goes through one `query(document, variables)` function: the real one is
`metamist.graphql.query`, and tests and the offline P1 (`parity/p1_offline.py`) pass a fake that answers from
canned responses.

Project names are passed in already suffixed (`-test` at access level test, `metamist_project`).
"""

import logging
import time
from collections.abc import Callable, Iterable, Mapping
from typing import Any

logger = logging.getLogger(__name__)

QueryFn = Callable[[Any, Mapping[str, Any]], dict[str, Any]]

# cpg_flow.metamist.COHORT_ACTIVE_CHECK
COHORT_ACTIVE_CHECK = """
    query CohortActiveQuery($cohorts: [String!]!) {
        cohorts(id: {in_: $cohorts}) {
            id
            status
        }
    }
"""

_SG_FIELDS = """
                id
                meta
                platform
                technology
                type
                sample {
                    project {
                        name
                    }
                    externalId
                    participant {
                        id
                        externalId
                        phenotypes
                        reportedSex
                        meta
                    }
                }
"""

# cpg_flow.metamist.GET_SEQUENCING_GROUPS_BY_COHORT_QUERY (assays dropped: only populate_assays reads them)
GET_SEQUENCING_GROUPS_BY_COHORT_QUERY = f"""
    query SGByCohortQuery($cohort_id: String!) {{
        cohorts(id: {{eq: $cohort_id}}) {{
            name
            status
            project {{
                dataset
            }}
            sequencingGroups {{{_SG_FIELDS}            }}
        }}
    }}
"""

# cpg_flow.metamist.GET_SEQUENCING_GROUPS_QUERY, plus sample.project.name, for the sample_evidence dataset input
GET_SEQUENCING_GROUPS_QUERY = f"""
    query SGQuery($metamist_proj: String!, $only_sgs: [String!]!, $skip_sgs: [String!]!, $sequencing_type: String!) {{
        project(name: $metamist_proj) {{
            sequencingGroups(
                id: {{ in_: $only_sgs, nin: $skip_sgs}}, type: {{eq: $sequencing_type}}
            ) {{{_SG_FIELDS}            }}
        }}
    }}
"""

# cpg_flow.metamist.GET_ANALYSES_QUERY
GET_ANALYSES_QUERY = """
    query AnalysesQuery($metamist_proj: String!, $analysis_type: String!, $analysis_status: AnalysisStatus!) {
        project(name: $metamist_proj) {
            analyses (active: {eq: true}, type: {eq: $analysis_type}, status: {eq: $analysis_status}) {
                id
                type
                meta
                output
                status
                sequencingGroups {
                    id
                }
            }
        }
    }
"""

# cpg_flow.metamist.GET_PEDIGREE_QUERY
GET_PEDIGREE_QUERY = """
    query PedigreeQuery($metamist_proj: String!){
        project(name: $metamist_proj) {
            pedigree(replaceWithFamilyExternalIds: false)
        }
    }
"""

# cpg_flow_gatk_sv.utils.GET_ACTIVE_SGS
GET_ACTIVE_SGS = """
    query ActiveSGQuery($metamist_proj: String!, $only_sgs: [String!]!){
        project(name: $metamist_proj) {
            sequencingGroups(id: { in_: $only_sgs}, activeOnly: {eq: true}) {
                id
            }
        }
    }
"""

# cpg_flow_gatk_sv.utils.VCF_QUERY
SV_ANALYSES_QUERY = """
    query SvAnalysesQuery($dataset: String!) {
        project(name: $dataset) {
            analyses(active: {eq: true}, type: {eq: "sv"}, status: {eq: COMPLETED}) {
                output
                timestampCompleted
            }
        }
    }
"""


class MetamistError(Exception):
    pass


def _graphql_query(document: str, variables: Mapping[str, Any]) -> dict[str, Any]:
    from metamist.graphql import gql, query  # noqa: PLC0415

    return query(gql(document), variables=dict(variables))


class Metamist:
    """The reads `plan` makes. `query_fn(document_text, variables)`; retries transport errors as cpg-flow does."""

    def __init__(self, query_fn: QueryFn | None = None, attempts: int = 3, backoff_seconds: float = 8.0) -> None:
        self._query_fn = query_fn or _graphql_query
        self._attempts = attempts
        self._backoff = backoff_seconds

    def _query(self, document: str, variables: Mapping[str, Any]) -> dict[str, Any]:
        for attempt in range(1, self._attempts + 1):
            try:
                return self._query_fn(document, variables)
            except Exception as e:  # gql's TransportServerError; anything else is re-raised below
                if type(e).__name__ != 'TransportServerError' or attempt == self._attempts:
                    raise
                logger.warning('metamist query failed (%s), attempt %d of %d; retrying', e, attempt, self._attempts)
                time.sleep(self._backoff * attempt)
        raise AssertionError('unreachable')

    def cohort_statuses(self, cohort_ids: Iterable[str]) -> dict[str, str]:
        result = self._query(COHORT_ACTIVE_CHECK, {'cohorts': list(cohort_ids)})
        return {c['id']: c['status'] for c in result['cohorts']}

    def cohort(self, cohort_id: str) -> dict[str, Any]:
        """Port of `cpg_flow.metamist.get_cohort_sgs`: `{name, dataset, sequencing_groups}`; refuses inactive."""
        entries = self._query(GET_SEQUENCING_GROUPS_BY_COHORT_QUERY, {'cohort_id': cohort_id})
        if len(entries['cohorts']) != 1:
            raise MetamistError('We only support one cohort at a time currently')
        cohort = entries['cohorts'][0]
        if cohort['status'].lower() != 'active':
            raise MetamistError(
                f'Cohort {cohort_id} is {cohort["status"]}. Only active cohorts are allowed. '
                'Please check the input cohort list.'
            )
        return {
            'name': cohort['name'],
            'dataset': cohort['project']['dataset'],
            'sequencing_groups': cohort['sequencingGroups'],
        }

    def project_sequencing_groups(
        self, project: str, *, only_sgs: list[str], skip_sgs: list[str], sequencing_type: str
    ) -> list[dict[str, Any]]:
        result = self._query(
            GET_SEQUENCING_GROUPS_QUERY,
            {'metamist_proj': project, 'only_sgs': only_sgs, 'skip_sgs': skip_sgs, 'sequencing_type': sequencing_type},
        )
        return result['project']['sequencingGroups']

    def completed_analyses(self, project: str, analysis_type: str) -> list[dict[str, Any]]:
        result = self._query(
            GET_ANALYSES_QUERY,
            {'metamist_proj': project, 'analysis_type': analysis_type, 'analysis_status': 'COMPLETED'},
        )
        return result['project']['analyses']

    def pedigree(self, project: str) -> list[dict[str, Any]]:
        return self._query(GET_PEDIGREE_QUERY, {'metamist_proj': project})['project']['pedigree']

    def active_sg_ids(self, project: str, sg_ids: list[str]) -> list[str]:
        result = self._query(GET_ACTIVE_SGS, {'metamist_proj': project, 'only_sgs': sg_ids})
        return [sg['id'] for sg in result['project']['sequencingGroups']]

    def sv_analyses(self, project: str) -> list[dict[str, Any]]:
        return self._query(SV_ANALYSES_QUERY, {'dataset': project})['project']['analyses']
