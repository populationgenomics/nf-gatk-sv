"""Parity with cpg_utils.cromwell.run_cromwell_workflow, via the fixture generated in parity/."""

import json
from pathlib import Path
from typing import Any

import pytest

from cpg_cromwell.options import REDACTED, build_workflow_options, options_for_key, redact_options
from cpg_cromwell.reattach import reattach_key
from cpg_cromwell.run import render
from cpg_cromwell.spec import WorkflowSource, WorkflowSpec
from tests.conftest import PARITY, SECRET


def _spec_for(case: dict[str, Any]) -> WorkflowSpec:
    return WorkflowSpec(
        name=case['name'],
        source=WorkflowSource('gs://b/bundles/repo/sha/', 'Wfl.wdl'),
        inputs={'some_input': 'gs://x/y', 'n': 3},
        input_prefix='Wfl',
        dataset=case['dataset'],
        access_level=case['access_level'],
        output_prefix=case['output_prefix'],
        google_project=case['project'],
        ar_guid=case['ar_guid_override'],
        labels=case['labels'] or {},
        copy_outputs=case['copy_outputs_to_gcp'],
    )


@pytest.mark.parametrize('case', PARITY['cases'], ids=lambda c: c['name'])
def test_options_match_run_cromwell_workflow(case: dict[str, Any], cpg_config: Path) -> None:
    expected = json.loads(case['options_json'])
    got = build_workflow_options(_spec_for(case), SECRET)
    assert got == expected
    assert list(got) == list(expected)  # same key order, so json.dumps gives the same bytes
    assert json.dumps(got) == case['options_json']


@pytest.mark.parametrize('case', PARITY['cases'], ids=lambda c: c['name'])
def test_inputs_json_matches(case: dict[str, Any], cpg_config: Path) -> None:
    spec = _spec_for(case)
    assert json.dumps(spec.prefixed_inputs()) == case['inputs_json']


def _decode(value: Any) -> Any:
    from cpg_utils import to_path

    if isinstance(value, dict) and set(value) == {'__path__'}:
        return to_path(value['__path__'])
    if isinstance(value, dict) and set(value) == {'__set__'}:
        return set(value['__set__'])
    if isinstance(value, dict):
        return {k: _decode(v) for k, v in value.items()}
    if isinstance(value, list):
        return [_decode(v) for v in value]
    return value


@pytest.mark.parametrize('case', PARITY['input_cases'], ids=lambda c: c['wfl_name'])
def test_input_serialisation_matches_add_gatk_sv_jobs(case: dict[str, Any]) -> None:
    spec = WorkflowSpec(
        name='x',
        source=WorkflowSource('gs://b/x/', 'X.wdl'),
        inputs=_decode(case['inputs']),
        input_prefix=case['wfl_name'],
        dataset='d',
        access_level='test',
        output_prefix='o',
    )
    assert spec.prefixed_inputs() == case['expected_input_dict']
    assert json.dumps(spec.prefixed_inputs()) == json.dumps(case['expected_input_dict'])


def test_render_redacts_secret_and_shows_labels(cpg_config: Path) -> None:
    case = PARITY['cases'][0]
    inputs, options = render(_spec_for(case), secret_json=SECRET)
    assert options['user_service_account_json'] == REDACTED
    assert 'PLACEHOLDER' not in json.dumps(options)
    assert options['google_labels']['ar-guid'] == case['ar_guid_override']
    assert inputs == {'Wfl.some_input': 'gs://x/y', 'Wfl.n': 3}


def test_redact_and_key_options() -> None:
    options = {'user_service_account_json': 's', 'google_labels': {'ar-guid': 'a'}, 'backend': 'batch'}
    assert redact_options(options)['user_service_account_json'] == REDACTED
    assert options_for_key(options) == {'backend': 'batch'}


def test_missing_client_email_is_spec_error(cpg_config: Path) -> None:
    from cpg_cromwell.errors import SpecError

    with pytest.raises(SpecError, match='client_email'):
        build_workflow_options(_spec_for(PARITY['cases'][0]), json.dumps({'type': 'service_account'}))


def test_missing_config_is_spec_error(monkeypatch: pytest.MonkeyPatch) -> None:
    from cpg_utils import config as cfg

    from cpg_cromwell.errors import SpecError

    monkeypatch.delenv('CPG_CONFIG_PATH', raising=False)
    monkeypatch.setattr(cfg, '_config_paths', [])
    monkeypatch.setattr(cfg, '_config', None)
    with pytest.raises(SpecError, match='config'):
        build_workflow_options(_spec_for(PARITY['cases'][0]), SECRET)


def test_reattach_key_ignores_labels_guid_and_secret(cpg_config: Path) -> None:
    case = PARITY['cases'][0]
    a = _spec_for(case)
    b = WorkflowSpec.from_dict({**a.to_dict(), 'ar_guid': 'another-launch', 'labels': {'stage': 'other'}})
    oa = build_workflow_options(a, SECRET)
    ob = build_workflow_options(b, SECRET.replace('PLACEHOLDER', 'DIFFERENT'))
    assert reattach_key('r', 'c', 'Wfl.wdl', a.prefixed_inputs(), oa) == reattach_key(
        'r', 'c', 'Wfl.wdl', b.prefixed_inputs(), ob
    )
    c = WorkflowSpec.from_dict({**a.to_dict(), 'inputs': {'some_input': 'gs://x/z', 'n': 3}})
    assert reattach_key('r', 'c', 'Wfl.wdl', c.prefixed_inputs(), oa) != reattach_key(
        'r', 'c', 'Wfl.wdl', a.prefixed_inputs(), oa
    )
    assert reattach_key('r', 'c2', 'Wfl.wdl', a.prefixed_inputs(), oa) != reattach_key(
        'r', 'c', 'Wfl.wdl', a.prefixed_inputs(), oa
    )
