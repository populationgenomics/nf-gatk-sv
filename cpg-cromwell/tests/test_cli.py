import json
from pathlib import Path

import pytest

from cpg_cromwell.cli import main
from cpg_cromwell.run import RunContext, result_filename
from tests.conftest import DirStorage, FakeCromwell, make_spec


def test_render(ctx: RunContext, tmp_path: Path, capsys: pytest.CaptureFixture[str]) -> None:
    spec_path = tmp_path / 'spec.json'
    spec_path.write_text(make_spec().to_json())
    assert main(['render', '--spec', str(spec_path)], context=ctx) == 0
    out = json.loads(capsys.readouterr().out)
    assert out['inputs'] == {'Wfl.x': 1, 'Wfl.vcf': 'gs://in/a.vcf'}
    assert out['options']['user_service_account_json'] == '<redacted>'
    assert out['options']['google_labels']['ar-guid'] == 'guid-1'


def test_bad_spec_exits_2(ctx: RunContext, tmp_path: Path) -> None:
    spec = make_spec().to_dict()
    spec['labels'] = {'ar-guid': 'x'}
    p = tmp_path / 'spec.json'
    p.write_text(json.dumps(spec))
    assert main(['run', '--spec', str(p)], context=ctx) == 2
    assert main(['run', '--spec', str(tmp_path / 'missing.json')], context=ctx) == 2


def test_run_writes_outputs_json_last(cromwell: FakeCromwell, ctx: RunContext, tmp_path: Path) -> None:
    p = tmp_path / 'spec.json'
    p.write_text(make_spec().to_json())
    outputs = tmp_path / 'outputs.json'
    result = tmp_path / 'result.json'
    code = main(['run', '--spec', str(p), '--outputs-json', str(outputs), '--result-json', str(result)], context=ctx)
    assert code == 0
    assert json.loads(outputs.read_text()) == {'out': 'gs://dest/out.txt'}
    assert json.loads(result.read_text())['status'] == 'Succeeded'


def test_run_failure_has_no_outputs_json(cromwell: FakeCromwell, ctx: RunContext, tmp_path: Path) -> None:
    cromwell.default_script = ['Failed']
    p = tmp_path / 'spec.json'
    p.write_text(make_spec().to_json())
    outputs = tmp_path / 'outputs.json'
    assert main(['run', '--spec', str(p), '--outputs-json', str(outputs)], context=ctx) == 1
    assert not outputs.exists()


def test_run_many_exit_code(cromwell: FakeCromwell, ctx: RunContext, storage: DirStorage, tmp_path: Path) -> None:
    cromwell.scripts = [['Succeeded'], ['Failed']]
    specs = tmp_path / 'specs.jsonl'
    specs.write_text('\n'.join(make_spec(name=f'w{i}', collect=None).to_json() for i in range(2)))
    code = main(
        ['run-many', '--specs', str(specs), '--results-dir', str(tmp_path / 'res'), '--submit-interval', '0'],
        context=ctx,
    )
    assert code == 1
    assert (tmp_path / 'res' / result_filename('w0')).exists()


def test_status_outputs_abort(cromwell: FakeCromwell, ctx: RunContext, capsys: pytest.CaptureFixture[str]) -> None:
    wid = cromwell.add(['Running'], outputs={'Wfl.out': 'gs://x'})
    assert main(['status', wid, '--json'], context=ctx) == 0
    assert json.loads(capsys.readouterr().out) == {'id': wid, 'status': 'Running'}
    assert main(['outputs', wid], context=ctx) == 0
    assert json.loads(capsys.readouterr().out) == {'Wfl.out': 'gs://x'}
    assert main(['metadata', wid, '--display'], context=ctx) == 0
    assert wid in capsys.readouterr().out
    assert main(['abort', wid], context=ctx) == 0
    assert cromwell.aborts == [wid]
    capsys.readouterr()
    # 'Aborting' isn't an ExecutionStatus in cpg_utils.cromwell_model: fall back to JSON
    assert main(['metadata', wid, '--display'], context=ctx) == 0
    assert json.loads(capsys.readouterr().out)['status'] == 'Aborting'
