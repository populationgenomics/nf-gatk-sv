from pathlib import Path, PurePosixPath

import pytest
from cloudpathlib import GSPath

from cpg_cromwell.errors import SpecError
from cpg_cromwell.spec import PollPolicy, WorkflowSource, WorkflowSpec, read_specs_jsonl
from tests.conftest import make_spec


def test_json_round_trip() -> None:
    spec = make_spec(
        inputs={'p': PurePosixPath('/a/b'), 's': {'b', 'a'}, 'l': (1, 2), 'd': {'k': [1]}},
        collect={'one': 'gs://x/1', 'many': ['gs://x/a', 'gs://x/b']},
        poll=PollPolicy(min_seconds=30, max_seconds=140, jitter=True, time_limit_seconds=86400),
    )
    again = WorkflowSpec.from_json(spec.to_json())
    assert again == spec
    assert again.inputs == {'p': '/a/b', 's': ['a', 'b'], 'l': ['1', '2'], 'd': {'k': [1]}}


def test_cloudpath_inputs_are_stringified_without_io() -> None:
    spec = make_spec(inputs={'cram': GSPath('gs://bucket/x.cram')})
    assert spec.inputs == {'cram': 'gs://bucket/x.cram'}


def test_prefixed_inputs() -> None:
    spec = make_spec(inputs={'a': 1}, input_prefix='GatherSampleEvidence')
    assert spec.prefixed_inputs() == {'GatherSampleEvidence.a': 1}
    assert make_spec(inputs={'a': 1}, input_prefix=None).prefixed_inputs() == {'a': 1}


def test_unknown_and_missing_fields() -> None:
    data = make_spec().to_dict()
    with pytest.raises(SpecError, match='unknown'):
        WorkflowSpec.from_dict({**data, 'bogus': 1})
    data.pop('dataset')
    with pytest.raises(SpecError, match='missing'):
        WorkflowSpec.from_dict(data)
    with pytest.raises(SpecError, match='not valid JSON'):
        WorkflowSpec.from_json('{')


def test_workflow_path_must_stay_inside_bundle() -> None:
    for bad in ('../x.wdl', '/abs.wdl', ''):
        with pytest.raises(SpecError):
            WorkflowSource('gs://b/x/', bad)


@pytest.mark.parametrize(
    ('labels', 'ar_guid', 'match'),
    [
        ({'ar-guid': 'x'}, None, 'ar-guid'),
        ({'compute-category': 'x'}, None, 'compute-category'),
        ({'stage': 'Has Space'}, None, 'invalid value'),
        ({'stage': 'UPPER'}, None, 'invalid value'),
        ({'Stage': 'x'}, None, 'invalid label key'),
        ({'1stage': 'x'}, None, 'invalid label key'),
        ({'stage': 'a' * 64}, None, 'invalid value'),
        ({}, 'GUID-UPPER', 'invalid value'),
        ({}, 'g' * 64, 'invalid value'),
        ({f'k{i}': 'v' for i in range(63)}, 'guid', 'too many labels'),
    ],
)
def test_label_validation(labels: dict, ar_guid: str | None, match: str) -> None:
    with pytest.raises(SpecError, match=match):
        make_spec(labels=labels, ar_guid=ar_guid)


def test_label_limits_accept_boundary() -> None:
    make_spec(labels={'stage': 'a' * 63}, ar_guid='g' * 63)
    make_spec(labels={f'k{i}': 'v' for i in range(62)}, ar_guid='guid')  # 62 + 2 = 64


def test_collect_validation() -> None:
    with pytest.raises(SpecError):
        make_spec(collect={'x': []})
    with pytest.raises(SpecError):
        make_spec(collect={'x': 3})
    assert make_spec(collect={'x': Path('/tmp/a')}).collect == {'x': '/tmp/a'}


def test_poll_validation() -> None:
    with pytest.raises(SpecError):
        PollPolicy(min_seconds=10, max_seconds=5)
    with pytest.raises(SpecError):
        PollPolicy(max_consecutive_errors=0)


def test_jsonl_unique_names() -> None:
    line = make_spec().to_json()
    with pytest.raises(SpecError, match='unique'):
        read_specs_jsonl(f'{line}\n{line}\n')
    assert len(read_specs_jsonl(f'{line}\n\n')) == 1
