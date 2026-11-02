"""SIGTERM/SIGINT delivered to a `cpg-cromwell run` subprocess abort the workflow (design §7.6)."""

import json
import os
import signal
import subprocess
import sys
import time
from pathlib import Path

import pytest

from cpg_cromwell.errors import EXIT_SIGTERM
from cpg_cromwell.run import RunContext, run
from tests.conftest import DirStorage, FakeCromwell, make_spec, write_bundle

ENTRY = Path(__file__).parent / '_cli_entry.py'


def _wait_for(predicate, timeout: float = 20) -> None:  # type: ignore[no-untyped-def]
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if predicate():
            return
        time.sleep(0.05)
    raise AssertionError('timed out waiting')


@pytest.mark.parametrize(('sig', 'code'), [(signal.SIGTERM, 143), (signal.SIGINT, 130)])
def test_signal_aborts_workflow(
    sig: signal.Signals,
    code: int,
    cromwell: FakeCromwell,
    storage: DirStorage,
    cpg_config: Path,
    tmp_path: Path,
) -> None:
    write_bundle(storage)
    cromwell.default_script = ['Running']
    spec_path = tmp_path / 'spec.json'
    spec_path.write_text(make_spec().to_json())
    proc = subprocess.Popen(
        [sys.executable, str(ENTRY), cromwell.url, str(storage.local.root), 'run', '--spec', str(spec_path)],  # type: ignore[attr-defined]
        env={**os.environ, 'CPG_CONFIG_PATH': str(cpg_config)},
        stderr=subprocess.PIPE,
        text=True,
    )
    try:
        _wait_for(lambda: cromwell.submissions and cromwell.workflows[cromwell.submissions[0]['id']].status_calls >= 2)
        proc.send_signal(sig)
        _, stderr = proc.communicate(timeout=30)
    finally:
        proc.kill()
    wid = cromwell.submissions[0]['id']
    assert proc.returncode == code, stderr
    assert cromwell.aborts == [wid]
    assert wid in stderr


@pytest.mark.parametrize(('sig', 'code'), [(signal.SIGTERM, 143), (signal.SIGINT, 130)])
def test_no_abort_on_exit_detaches_cleanly(
    sig: signal.Signals,
    code: int,
    cromwell: FakeCromwell,
    storage: DirStorage,
    cpg_config: Path,
    tmp_path: Path,
) -> None:
    """--no-abort-on-exit still traps the signal: no abort, but a clean exit with a result written."""
    write_bundle(storage)
    cromwell.default_script = ['Running']
    spec_path = tmp_path / 'spec.json'
    spec_path.write_text(make_spec().to_json())
    result_path = tmp_path / 'result.json'
    proc = subprocess.Popen(
        [
            sys.executable,
            str(ENTRY),
            cromwell.url,
            str(storage.local.root),  # type: ignore[attr-defined]
            'run',
            '--no-abort-on-exit',
            '--spec',
            str(spec_path),
            '--result-json',
            str(result_path),
        ],
        env={**os.environ, 'CPG_CONFIG_PATH': str(cpg_config)},
        stderr=subprocess.PIPE,
        text=True,
    )
    try:
        _wait_for(lambda: cromwell.submissions and cromwell.workflows[cromwell.submissions[0]['id']].status_calls >= 2)
        proc.send_signal(sig)
        _, stderr = proc.communicate(timeout=30)
    finally:
        proc.kill()
    wid = cromwell.submissions[0]['id']
    assert proc.returncode == code, stderr
    assert cromwell.aborts == []
    result = json.loads(result_path.read_text())
    assert result['workflow_id'] == wid
    assert result['exit_code'] == code
    assert result['status'] == 'Running'
    assert result['error'] == 'detached on signal (workflow left running)'


def _signal_on_status_call(ctx: RunContext, n: int, before_signal=None) -> None:  # type: ignore[no-untyped-def]
    """Send this process SIGTERM during the n-th status poll (the handler runs on the main thread)."""
    real_status = ctx.api.status  # type: ignore[union-attr]
    calls = 0

    def status(wid: str) -> str:
        nonlocal calls
        calls += 1
        value = real_status(wid)
        if calls == n:
            if before_signal is not None:
                before_signal()
            os.kill(os.getpid(), signal.SIGTERM)
        return value

    ctx.api.status = status  # type: ignore[method-assign,union-attr]


def test_detach_records_live_status(cromwell: FakeCromwell, ctx: RunContext) -> None:
    cromwell.default_script = ['Submitted', 'Running']
    _signal_on_status_call(ctx, 2)
    result = run(make_spec(), abort_on_signal=False, context=ctx)
    assert (result.status, result.exit_code) == ('Running', EXIT_SIGTERM)
    assert result.error == 'detached on signal (workflow left running)'
    assert cromwell.aborts == []


def test_detach_records_terminal_status(cromwell: FakeCromwell, ctx: RunContext) -> None:
    # the workflow finishes between the last poll and the detach check
    cromwell.default_script = ['Running', 'Running', 'Succeeded']
    _signal_on_status_call(ctx, 2)
    result = run(make_spec(), abort_on_signal=False, context=ctx)
    assert (result.status, result.exit_code) == ('Succeeded', EXIT_SIGTERM)
    assert result.error == 'detached on signal; the workflow had already finished (Succeeded)'
    assert result.outputs is None  # not collected: the process was told to stop


def test_detach_when_cromwell_unreachable(cromwell: FakeCromwell, ctx: RunContext) -> None:
    cromwell.default_script = ['Running']

    def go_down() -> None:
        cromwell.fail_next = [503]

    _signal_on_status_call(ctx, 1, before_signal=go_down)
    result = run(make_spec(), abort_on_signal=False, context=ctx)
    assert (result.status, result.exit_code) == ('Unreachable', EXIT_SIGTERM)
    assert result.error.startswith('detached on signal; could not check the workflow status')
    assert result.workflow_id is not None
    assert cromwell.aborts == []
