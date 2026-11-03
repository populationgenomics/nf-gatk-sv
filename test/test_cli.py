import argparse
import json
from pathlib import Path
from typing import Any

import pytest
from conftest import BASE_CONFIG, COMMIT

from gatk_sv_nf import cli
from gatk_sv_nf.cromwell import RunContext
from gatk_sv_nf.errors import EXIT_OK
from gatk_sv_nf.stages._cromwell_base import default_bundle


def _plan_dir(tmp_path: Path) -> Path:
    plan = tmp_path / 'plan'
    plan.mkdir()
    (plan / 'config.toml').write_text(BASE_CONFIG)
    return plan


@pytest.fixture
def captured_bundle(monkeypatch: pytest.MonkeyPatch) -> dict[str, Any]:
    calls: dict[str, Any] = {}

    def fake_build_bundle(repo: str, commit: str, **kwargs: Any) -> str:
        calls.update(repo=repo, commit=commit, **kwargs)
        return f'{kwargs["dest"]}/{repo}/{commit}/'

    monkeypatch.setattr(cli, 'build_bundle', fake_build_bundle)
    return calls


@pytest.mark.usefixtures('write_config')  # resets cpg_utils' config cache afterwards
def test_prepare_wdl_bundles_where_stages_look(
    tmp_path: Path, captured_bundle: dict[str, Any], capsys: pytest.CaptureFixture[str]
) -> None:
    plan = _plan_dir(tmp_path)
    code = cli.cmd_prepare_wdl(argparse.Namespace(plan=str(plan)), RunContext(storage=object()))  # type: ignore[arg-type]
    assert code == EXIT_OK

    printed = capsys.readouterr().out.strip()
    # the URI the stages default to (without --bundle) is the one PREPARE_WDL made
    assert printed == default_bundle()
    assert captured_bundle['repo'] == 'gatk-sv'
    assert captured_bundle['commit'] == COMMIT
    assert captured_bundle['cwd'] == 'wdl'
    assert captured_bundle['libs'] == ['.']
    assert captured_bundle['dest'] == 'gs://cpg-analysis-ds-test-tmp/gatk_sv_nf/bundles'
    assert captured_bundle['require_main'] is False  # BASE_CONFIG is access_level test


@pytest.mark.usefixtures('write_config')
def test_prepare_wdl_requires_main_outside_test(tmp_path: Path, captured_bundle: dict[str, Any]) -> None:
    plan = _plan_dir(tmp_path)
    # BASE_CONFIG already has a [workflow] table, so rewrite the key rather than append one
    text = (plan / 'config.toml').read_text().replace('access_level = "test"', 'access_level = "standard"')
    (plan / 'config.toml').write_text(text)
    cli.cmd_prepare_wdl(argparse.Namespace(plan=str(plan)), RunContext(storage=object()))  # type: ignore[arg-type]
    assert captured_bundle['require_main'] is True


@pytest.mark.usefixtures('write_config')
def test_dump_inputs_has_everything_p3_compares(tmp_path: Path) -> None:
    plan = _plan_dir(tmp_path)
    sg = {'id': 'CPG111', 'dataset': 'fixture-ds', 'cram': 'gs://cpg-fixture-ds-test/cram/CPG111.cram'}
    (plan / 'manifest.json').write_text(json.dumps({'sequencing_groups': [sg]}))
    (plan / 'chunk.json').write_text(json.dumps(['CPG111']))
    dump = tmp_path / 'dump.json'

    code = cli.main(
        ['stage', 'GatherSampleEvidence', '--plan', str(plan), '--chunk', str(plan / 'chunk.json'),
         '--ar-guid', 'guid-1', '--bundle', 'gs://b/gatk-sv/c/', '--dump-inputs', str(dump)]
    )  # fmt: skip
    assert code == EXIT_OK

    entry = json.loads(dump.read_text())['CPG111']
    assert set(entry) == {
        'inputs', 'options', 'collect', 'workflow', 'dataset', 'access_level', 'output_prefix', 'copy_outputs',
        'poll', 'time_limit_seconds',
    }  # fmt: skip
    assert entry['inputs']['GatherSampleEvidence.sample_id'] == 'CPG111'
    assert entry['options']['google_labels']['ar-guid'] == 'guid-1'
    assert entry['options']['user_service_account_json'] == '<redacted>'
    assert entry['workflow'] == 'GatherSampleEvidence.wdl'
    assert entry['dataset'] == 'analysis-ds'  # workflow.dataset, not the SG's
    assert entry['output_prefix'] == 'gatk_sv/output/GatherSampleEvidence/fixture-ds/CPG111'
    # BASE_CONFIG overrides the large bounds; these are before jitter
    assert entry['poll'] == {'min': 1, 'max': 2}
    assert entry['time_limit_seconds'] == 86400
