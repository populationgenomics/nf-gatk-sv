"""End-to-end run/run_many against the fake Cromwell server and directory-backed storage."""

import json

import pytest

from cpg_cromwell.errors import EXIT_FAILED, EXIT_OK, EXIT_TIMEOUT, EXIT_TRANSIENT, EXIT_USAGE, combine_exit_codes
from cpg_cromwell.reattach import ReattachRecord, ReattachStore, reattach_key
from cpg_cromwell.run import RunContext, result_filename, run, run_many
from cpg_cromwell.spec import PollPolicy, WorkflowResult
from tests.conftest import FAST, SECRET, DirStorage, FakeCromwell, make_spec, write_bundle

ROOT = 'gs://tmp/cromwell_ids'


def test_success_submits_and_collects(cromwell: FakeCromwell, ctx: RunContext, storage: DirStorage) -> None:
    result = run(make_spec(), context=ctx)
    assert (result.status, result.exit_code, result.reattached) == ('Succeeded', EXIT_OK, False)
    assert result.outputs == {'out': 'gs://dest/out.txt'}
    assert result.submitted_ar_guid == 'guid-1'
    assert storage.text('gs://dest/out.txt') == 'hello'

    [sub] = cromwell.submissions
    assert json.loads(sub['workflowInputs']) == {'Wfl.x': 1, 'Wfl.vcf': 'gs://in/a.vcf'}
    options = json.loads(sub['workflowOptions'])
    assert options['user_service_account_json'] == SECRET
    assert options['google_labels'] == {'stage': 'wfl', 'compute-category': 'cromwell', 'ar-guid': 'guid-1'}
    assert sub['workflowSource'].startswith(b'version 1.0')
    assert sub['workflowDependencies'][:2] == b'PK'


def test_failed_workflow(cromwell: FakeCromwell, ctx: RunContext) -> None:
    cromwell.default_script = ['Running', 'Failed']
    cromwell.default_failures = [
        {'message': 'Workflow failed', 'causedBy': [{'message': 'Task X OOM', 'causedBy': []}]}
    ]
    result = run(make_spec(), context=ctx)
    assert (result.status, result.exit_code) == ('Failed', EXIT_FAILED)
    assert result.failures == ['Workflow failed, caused by: Task X OOM']


def test_aborted_elsewhere_is_failure(cromwell: FakeCromwell, ctx: RunContext) -> None:
    cromwell.default_script = ['Running', 'Aborted']
    result = run(make_spec(), context=ctx)
    assert (result.status, result.exit_code) == ('Aborted', EXIT_FAILED)


def test_transient_503_burst_is_survived(cromwell: FakeCromwell, ctx: RunContext) -> None:
    cromwell.default_script = ['Running', 'Running', 'Succeeded']
    cromwell.fail_next = [503, 503, 503]  # hits the submission: retried
    result = run(make_spec(), context=ctx)
    assert result.exit_code == EXIT_OK
    assert len(cromwell.submissions) == 1


def test_unreachable_exits_75_without_abort(cromwell: FakeCromwell, ctx: RunContext) -> None:
    cromwell.default_script = ['Running']
    real_submit = ctx.api.submit

    def submit_then_go_down(*a, **k):  # type: ignore[no-untyped-def]
        wid = real_submit(*a, **k)
        cromwell.fail_next = [503] * 100  # every poll after the submission fails
        return wid

    ctx.api.submit = submit_then_go_down  # type: ignore[method-assign]
    result = run(make_spec(), context=ctx)
    assert (result.status, result.exit_code) == ('Unreachable', EXIT_TRANSIENT)
    assert result.workflow_id is not None
    assert cromwell.aborts == []  # left running so a retry can re-attach


def test_submission_rejected_is_usage_error(cromwell: FakeCromwell, ctx: RunContext) -> None:
    cromwell.submit_status = 400
    result = run(make_spec(), context=ctx)
    assert (result.status, result.exit_code) == ('NotSubmitted', EXIT_USAGE)


def test_time_limit_aborts_and_exits_124(cromwell: FakeCromwell, ctx: RunContext) -> None:
    cromwell.default_script = ['Running']
    spec = make_spec(poll=PollPolicy(min_seconds=0.01, max_seconds=0.02, ramp_seconds=1, time_limit_seconds=0.2))
    result = run(spec, context=ctx)
    assert (result.status, result.exit_code) == ('TimedOut', EXIT_TIMEOUT)
    assert cromwell.aborts == [result.workflow_id]
    assert cromwell.status_of(result.workflow_id) == 'Aborted'  # type: ignore[arg-type]


def test_unverified_bundle_refused_for_non_test(cromwell: FakeCromwell, ctx: RunContext, storage: DirStorage) -> None:
    write_bundle(storage, verified=False)
    result = run(make_spec(access_level='standard'), context=ctx)
    assert (result.status, result.exit_code) == ('NotSubmitted', EXIT_USAGE)
    assert 'not verified on main' in (result.error or '')
    assert cromwell.submissions == []
    assert run(make_spec(access_level='test'), context=ctx).exit_code == EXIT_OK


def test_missing_secret_is_usage_error(cromwell: FakeCromwell, ctx: RunContext) -> None:
    from cpg_cromwell.errors import SpecError

    def no_secret(ds: str, al: str) -> str:
        raise SpecError(f"couldn't read secret {ds}-cromwell-{al}-key")

    ctx.secret_reader = no_secret
    result = run(make_spec(), context=ctx)
    assert (result.status, result.exit_code) == ('NotSubmitted', EXIT_USAGE)


def test_strict_collection_failure(cromwell: FakeCromwell, ctx: RunContext) -> None:
    cromwell.default_outputs = {'Wfl.other': 'gs://exec/out.txt'}
    result = run(make_spec(), context=ctx)
    assert (result.status, result.exit_code) == ('Succeeded', EXIT_FAILED)
    assert 'Wfl.out is missing' in (result.error or '')


# ---- re-attach -------------------------------------------------------------------------------


def _seed_record(ctx: RunContext, spec, workflow_id: str, ar_guid: str = 'old-guid') -> None:  # type: ignore[no-untyped-def]
    from cpg_cromwell.options import build_workflow_options

    key = reattach_key(
        'repo',
        '0123456789abcdef0123456789abcdef01234567',
        spec.source.workflow,
        spec.prefixed_inputs(),
        build_workflow_options(spec, SECRET),
    )
    ReattachStore(ROOT, ctx.storage).write(
        key, ReattachRecord(workflow_id, spec.name, 't0', ar_guid), replace_generation=None
    )  # type: ignore[arg-type]


def test_reattach_running(cromwell: FakeCromwell, ctx: RunContext) -> None:
    spec = make_spec(ar_guid='new-guid')
    wid = cromwell.add(['Running', 'Running', 'Succeeded'])
    _seed_record(ctx, spec, wid)
    result = run(spec, reattach_root=ROOT, context=ctx)
    assert (result.exit_code, result.workflow_id, result.reattached, result.submitted_ar_guid) == (
        EXIT_OK,
        wid,
        True,
        'old-guid',
    )
    assert cromwell.submissions == []


def test_reattach_succeeded_goes_straight_to_collect(cromwell: FakeCromwell, ctx: RunContext) -> None:
    spec = make_spec()
    wid = cromwell.add(['Succeeded'])
    _seed_record(ctx, spec, wid)
    result = run(spec, reattach_root=ROOT, context=ctx)
    assert (result.exit_code, result.workflow_id, result.reattached) == (EXIT_OK, wid, True)
    assert cromwell.submissions == []
    assert cromwell.workflows[wid].status_calls == 1


@pytest.mark.parametrize('status', ['Failed', 'Aborted'])
def test_reattach_failed_or_aborted_resubmits_and_replaces_record(
    cromwell: FakeCromwell, ctx: RunContext, status: str
) -> None:
    spec = make_spec(ar_guid='new-guid')
    old = cromwell.add([status])
    _seed_record(ctx, spec, old)
    result = run(spec, reattach_root=ROOT, context=ctx)
    assert result.exit_code == EXIT_OK
    assert result.workflow_id != old
    assert (result.reattached, result.submitted_ar_guid) == (False, 'new-guid')
    # the record now points at the new workflow, so a further relaunch attaches to it
    again = run(spec, reattach_root=ROOT, context=ctx)
    assert (again.workflow_id, again.reattached) == (result.workflow_id, True)
    assert len(cromwell.submissions) == 1


def test_reattach_404_resubmits(cromwell: FakeCromwell, ctx: RunContext) -> None:
    spec = make_spec()
    _seed_record(ctx, spec, 'purged-workflow-id')
    result = run(spec, reattach_root=ROOT, context=ctx)
    assert (result.exit_code, result.reattached) == (EXIT_OK, False)
    assert len(cromwell.submissions) == 1


def test_reattach_aborting_waits_then_resubmits(cromwell: FakeCromwell, ctx: RunContext) -> None:
    spec = make_spec()
    old = cromwell.add(['Aborting', 'Aborting', 'Aborting', 'Aborted'])
    _seed_record(ctx, spec, old)
    result = run(spec, reattach_root=ROOT, context=ctx)
    assert result.exit_code == EXIT_OK
    assert result.workflow_id != old
    assert cromwell.workflows[old].status_calls >= 4


def test_lost_record_race_aborts_duplicate_and_attaches(cromwell: FakeCromwell, ctx: RunContext) -> None:
    spec = make_spec()
    theirs = cromwell.add(['Running', 'Succeeded'])
    real_submit = ctx.api.submit

    def submit_and_get_beaten(*a, **k):  # type: ignore[no-untyped-def]
        wid = real_submit(*a, **k)
        _seed_record(ctx, spec, theirs, ar_guid='other-process')  # they create the record first
        return wid

    ctx.api.submit = submit_and_get_beaten  # type: ignore[method-assign]
    result = run(spec, reattach_root=ROOT, context=ctx)
    [ours] = cromwell.submissions
    assert cromwell.aborts == [ours['id']]
    assert (result.exit_code, result.workflow_id, result.reattached) == (EXIT_OK, theirs, True)


def test_without_reattach_root_always_submits(cromwell: FakeCromwell, ctx: RunContext) -> None:
    run(make_spec(), context=ctx)
    run(make_spec(), context=ctx)
    assert len(cromwell.submissions) == 2


# ---- run_many --------------------------------------------------------------------------------


def test_run_many_mixed_results(cromwell: FakeCromwell, ctx: RunContext, storage: DirStorage) -> None:
    cromwell.scripts = [['Running', 'Succeeded'], ['Running', 'Failed'], ['Running', 'Succeeded']]
    specs = [make_spec(name=f'GSE/cpg{i}', collect={'out': f'gs://dest/{i}.txt'}) for i in range(3)]
    seen: list[str] = []
    results = run_many(
        specs,
        results_dir='gs://tmp/results',
        submit_interval=0.01,
        on_result=lambda r: seen.append(r.name),
        context=ctx,
    )
    assert [r.status for r in results] == ['Succeeded', 'Failed', 'Succeeded']
    assert combine_exit_codes([r.exit_code for r in results]) == EXIT_FAILED
    assert sorted(seen) == sorted(s.name for s in specs)
    for spec, r in zip(specs, results, strict=True):
        written = WorkflowResult.from_dict(json.loads(storage.text(f'gs://tmp/results/{result_filename(spec.name)}')))  # type: ignore[arg-type]
        assert written == r
    assert storage.text('gs://dest/0.txt') == 'hello'
    assert storage.text('gs://dest/1.txt') is None


def test_run_many_on_result_exception_fails_only_that_workflow(cromwell: FakeCromwell, ctx: RunContext) -> None:
    specs = [make_spec(name=f'w{i}', collect={'out': f'gs://dest/{i}.txt'}) for i in range(2)]

    def callback(r: WorkflowResult) -> None:
        if r.name == 'w0':
            raise RuntimeError('metamist down')

    results = run_many(specs, submit_interval=0, on_result=callback, context=ctx)
    assert [r.exit_code for r in results] == [EXIT_FAILED, EXIT_OK]
    assert 'metamist down' in (results[0].error or '')


def test_run_many_max_in_flight(cromwell: FakeCromwell, ctx: RunContext) -> None:
    cromwell.default_script = ['Running', 'Running', 'Running', 'Succeeded']
    specs = [make_spec(name=f'w{i}', collect=None) for i in range(4)]
    in_flight_at_submit: list[int] = []
    real_submit = ctx.api.submit

    def submit(*a, **k):  # type: ignore[no-untyped-def]
        running = sum(1 for w in cromwell.workflows.values() if w.current not in ('Succeeded', 'Failed', 'Aborted'))
        in_flight_at_submit.append(running)
        return real_submit(*a, **k)

    ctx.api.submit = submit  # type: ignore[method-assign]
    results = run_many(specs, submit_interval=0, max_in_flight=2, context=ctx)
    assert all(r.ok for r in results)
    assert max(in_flight_at_submit) <= 2


def test_run_many_rejects_duplicate_names(ctx: RunContext) -> None:
    from cpg_cromwell.errors import SpecError

    with pytest.raises(SpecError, match='unique'):
        run_many([make_spec(), make_spec()], context=ctx)


def test_shared_bundle_and_secret_are_cached(cromwell: FakeCromwell, ctx: RunContext) -> None:
    calls: list[tuple[str, str]] = []

    def secret(ds: str, al: str) -> str:
        calls.append((ds, al))
        return SECRET

    ctx.secret_reader = secret
    run_many([make_spec(name=f'w{i}', collect=None) for i in range(3)], submit_interval=0, context=ctx)
    assert calls == [('fixture-ds', 'test')]


def test_fast_policy_is_used() -> None:
    assert FAST.max_seconds < 1
