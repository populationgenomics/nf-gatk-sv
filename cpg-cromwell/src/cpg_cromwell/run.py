"""
Orchestration: render(), run() and run_many() (design §5, §7.4-§7.8).

`run` is `run_many([spec])`, so there is one code path. Each spec gets a WorkflowRunner, a small
state machine stepped by the single-threaded Scheduler in watch.py. Only output collection runs
on background threads (it can take minutes for multi-GB copies), and its completion wakes the loop.
"""

import enum
import hashlib
import logging
import random
import re
import sys
import threading
import time
from collections.abc import Callable, Sequence
from concurrent.futures import Future, ThreadPoolExecutor
from dataclasses import dataclass, field
from typing import Any, Self

from cpg_cromwell.api import (
    DISPLAY_METADATA_KEYS,
    TERMINAL_STATUSES,
    CromwellApi,
    CromwellNotFound,
    CromwellRequestError,
)
from cpg_cromwell.bundle import LoadedBundle, load_bundle
from cpg_cromwell.collect import DEFAULT_COPY_THREADS, collect_outputs
from cpg_cromwell.errors import (
    EXIT_FAILED,
    EXIT_OK,
    EXIT_TIMEOUT,
    EXIT_TRANSIENT,
    EXIT_USAGE,
    Cancelled,
    CromwellCliError,
    SpecError,
    TransientError,
)
from cpg_cromwell.options import build_workflow_options, read_cromwell_secret, redact_options
from cpg_cromwell.reattach import (
    Action,
    PreconditionFailed,
    ReattachRecord,
    ReattachStore,
    action_for,
    now_iso,
    reattach_key,
)
from cpg_cromwell.spec import Status, WorkflowResult, WorkflowSpec
from cpg_cromwell.storage import Storage, join
from cpg_cromwell.watch import PollClock, Scheduler, SignalTrap, SubmitGate

logger = logging.getLogger(__name__)

MAX_FAILURE_MESSAGES = 10
MAX_FAILURE_CHARS = 1000
MAX_RECORD_RACES = 3


@dataclass
class RunContext:
    """Injectable dependencies. Defaults talk to the real Cromwell, GCS and Secret Manager."""

    api: CromwellApi | None = None
    storage: Storage | None = None
    secret_reader: Callable[[str, str], str] = read_cromwell_secret
    clock: Callable[[], float] = time.monotonic
    rng: random.Random = field(default_factory=random.Random)
    collect_workers: int = 4
    copy_threads: int = DEFAULT_COPY_THREADS
    abort_confirm_seconds: float = 600  # bounded wait for Aborted after a time-limit abort
    abort_poll_seconds: float = 10

    def resolved(self) -> Self:
        if self.api is None:
            self.api = CromwellApi()
        if self.storage is None:
            self.storage = Storage()
        return self


def result_filename(name: str) -> str:
    """Safe, unique file name for a spec name (`<results_dir>/<this>`)."""
    safe = re.sub(r'[^A-Za-z0-9._-]+', '_', name).strip('_.') or 'workflow'
    return f'{safe[:100]}-{hashlib.sha256(name.encode()).hexdigest()[:8]}.json'


def render(
    spec: WorkflowSpec, *, context: RunContext | None = None, secret_json: str | None = None
) -> tuple[dict, dict]:
    """(prefixed inputs, workflow options with the secret redacted). No network writes."""
    ctx = context or RunContext()
    secret = secret_json if secret_json is not None else ctx.secret_reader(spec.dataset, spec.access_level)
    return spec.prefixed_inputs(), redact_options(build_workflow_options(spec, secret))


def extract_failures(metadata: dict[str, Any]) -> list[str]:
    from cpg_utils.cromwell_model import unwrap_caused_by  # noqa: PLC0415

    out = []
    for failure in (metadata.get('failures') or [])[:MAX_FAILURE_MESSAGES]:
        message = unwrap_caused_by([failure]) if isinstance(failure, dict) else str(failure)
        if message:
            out.append(message[:MAX_FAILURE_CHARS])
    return out


class Phase(enum.Enum):
    PREPARE = 'prepare'
    LOOKUP = 'lookup'
    SUBMIT = 'submit'
    WAIT_ABORTED = 'wait_aborted'
    WATCH = 'watch'
    ABORT = 'abort'
    COLLECTING = 'collecting'
    DONE = 'done'


class _Shared:
    """State shared by all runners in one run_many call. Only touched from the loop thread."""

    def __init__(
        self,
        ctx: RunContext,
        store: ReattachStore | None,
        gate: SubmitGate,
        wake: threading.Event,
        collector: ThreadPoolExecutor,
        on_finish: Callable[['WorkflowRunner'], None],
    ) -> None:
        self.ctx = ctx
        self.api: CromwellApi = ctx.api  # type: ignore[assignment]
        self.storage: Storage = ctx.storage  # type: ignore[assignment]
        self.store = store
        self.gate = gate
        self.wake = wake
        self.collector = collector
        self.on_finish = on_finish
        self._bundles: dict[tuple[str, str, bool], LoadedBundle] = {}
        self._secrets: dict[tuple[str, str], str] = {}

    def bundle(self, spec: WorkflowSpec) -> LoadedBundle:
        k = (spec.source.bundle_root, spec.source.workflow, spec.access_level == 'test')
        if k not in self._bundles:
            self._bundles[k] = load_bundle(spec.source, spec.access_level, self.storage)
        return self._bundles[k]

    def secret(self, dataset: str, access_level: str) -> str:
        k = (dataset, access_level)
        if k not in self._secrets:
            self._secrets[k] = self.ctx.secret_reader(dataset, access_level)
        return self._secrets[k]


class WorkflowRunner:
    def __init__(self, spec: WorkflowSpec, shared: _Shared) -> None:
        self.spec = spec
        self.sh = shared
        self.policy = spec.poll
        self.result = WorkflowResult(name=spec.name, workflow_id=None, status='NotSubmitted')
        self.phase = Phase.PREPARE
        self.next_at = 0.0
        self.errors = 0
        self.races = 0
        self.in_flight = False
        self.poll: PollClock | None = None
        self.limit_start = 0.0
        self.abort_sent = False
        self.deadline = 0.0
        self.record_generation: int | None = None
        self.cancel = threading.Event()
        self.future: Future[dict[str, Any]] | None = None
        # prepared
        self.bundle: LoadedBundle | None = None
        self.inputs: dict[str, Any] = {}
        self.options: dict[str, Any] = {}
        self.key = ''

    # ---- helpers -------------------------------------------------------------------------

    @property
    def done(self) -> bool:
        return self.phase is Phase.DONE

    @property
    def wid(self) -> str:
        return self.result.workflow_id or '?'

    def log(self, level: int, msg: str, *args: Any) -> None:
        logger.log(level, f'[{self.spec.name}] {msg}', *args)

    def _backoff(self) -> float:
        return max(0.01, min(self.policy.max_seconds, max(self.policy.min_seconds, 0.01) * 2 ** (self.errors - 1)))

    def _error(self, now: float, e: Exception, *, interval: float | None = None) -> bool:
        """Count a consecutive error. Returns True (and finishes as Unreachable) once the budget is spent."""
        self.errors += 1
        self.log(logging.WARNING, 'error %d/%d: %s', self.errors, self.policy.max_consecutive_errors, e)
        if self.errors >= self.policy.max_consecutive_errors:
            self.finish('Unreachable', EXIT_TRANSIENT, error=f'giving up after {self.errors} consecutive errors: {e}')
            return True
        self.next_at = now + (interval if interval is not None else self._backoff())
        return False

    def _release(self) -> None:
        if self.in_flight:
            self.in_flight = False
            self.sh.gate.in_flight -= 1
            self.sh.wake.set()  # a slot freed up for a waiting submission

    def finish(
        self,
        status: Status,
        exit_code: int,
        *,
        error: str | None = None,
        failures: list[str] | None = None,
        outputs: dict[str, Any] | None = None,
        notify: bool = True,
    ) -> None:
        self._release()
        self.result.status = status
        self.result.exit_code = exit_code
        self.result.error = error
        if failures is not None:
            self.result.failures = failures
        if outputs is not None:
            self.result.outputs = outputs
        self.phase = Phase.DONE
        level = logging.INFO if exit_code == EXIT_OK else logging.ERROR
        self.log(level, 'finished: %s (exit %d)%s', status, exit_code, f': {error}' if error else '')
        if notify:
            self.sh.on_finish(self)

    def _start_watch(self, now: float, workflow_id: str) -> None:
        self.result.workflow_id = workflow_id
        self.phase = Phase.WATCH
        self.poll = PollClock(self.policy, now, self.sh.ctx.rng)
        self.limit_start = now
        self.errors = 0
        if not self.in_flight:
            self.in_flight = True
            self.sh.gate.in_flight += 1
        self.next_at = now

    def _attach(self, record: ReattachRecord) -> None:
        self.result.workflow_id = record.workflow_id
        self.result.reattached = True
        self.result.submitted_ar_guid = record.ar_guid
        self.log(
            logging.INFO,
            're-attaching to %s (submitted %s by ar-guid %s; this launch is ar-guid %s)',
            record.workflow_id,
            record.submitted_at,
            record.ar_guid,
            self.spec.ar_guid,
        )

    # ---- the state machine --------------------------------------------------------------

    def step(self, now: float) -> None:
        handler = {
            Phase.PREPARE: self._prepare,
            Phase.LOOKUP: self._lookup,
            Phase.SUBMIT: self._submit,
            Phase.WAIT_ABORTED: self._wait_aborted,
            Phase.WATCH: self._watch,
            Phase.ABORT: self._abort_for_timeout,
            Phase.COLLECTING: self._collecting,
        }[self.phase]
        try:
            handler(now)
        except CromwellCliError as e:
            # anything not handled closer to the source: unexpected but classified
            self.finish(self.result.status, e.exit_code, error=str(e))

    def _prepare(self, now: float) -> None:
        spec = self.spec
        self.log(
            logging.INFO,
            'starting: workflow=%s bundle=%s dataset=%s access_level=%s ar_guid=%s',
            spec.source.workflow,
            spec.source.bundle_root,
            spec.dataset,
            spec.access_level,
            spec.ar_guid,
        )
        try:
            self.bundle = self.sh.bundle(spec)
            secret = self.sh.secret(spec.dataset, spec.access_level)
            self.options = build_workflow_options(spec, secret)
            self.inputs = spec.prefixed_inputs()
        except CromwellCliError as e:
            self.finish('NotSubmitted', e.exit_code, error=str(e))
            return
        m = self.bundle.manifest
        self.key = reattach_key(m.repo, m.commit, spec.source.workflow, self.inputs, self.options)
        self.log(logging.INFO, 'google_labels=%s', self.options['google_labels'])
        self.phase = Phase.LOOKUP if self.sh.store else Phase.SUBMIT
        self.next_at = now

    def _lookup(self, now: float) -> None:
        store = self.sh.store
        assert store is not None
        try:
            record = store.read(self.key)
        except TransientError as e:
            self._error(now, e)
            return
        if record is None:
            self.record_generation = None
            self.phase, self.next_at = Phase.SUBMIT, now
            return
        self.record_generation = record.generation
        status: str | None = None
        if record.workflow_id:
            try:
                status = self.sh.api.status(record.workflow_id)
            except TransientError as e:
                self._error(now, e)
                return
            except CromwellRequestError:  # 404 (purged) or malformed id
                status = None
        self.errors = 0
        action = action_for(status)
        self.log(
            logging.INFO,
            're-attach record %s: workflow %s is %s -> %s',
            self.key[:12],
            record.workflow_id,
            status,
            action.value,
        )
        if action is Action.SUBMIT:
            self.phase, self.next_at = Phase.SUBMIT, now
        elif action is Action.ATTACH:
            self._attach(record)
            self._start_watch(now, record.workflow_id)
        elif action is Action.COLLECT:
            self._attach(record)
            self._terminal('Succeeded')
        else:  # WAIT_ABORTED
            self.result.workflow_id = record.workflow_id
            self.phase = Phase.WAIT_ABORTED
            self.deadline = now + self.sh.ctx.abort_confirm_seconds
            self.next_at = now + self.sh.ctx.abort_poll_seconds

    def _submit(self, now: float) -> None:
        wait = self.sh.gate.wait_until(now)
        if wait is not None:
            self.next_at = wait
            return
        self.sh.gate.submitted(now)
        b = self.bundle
        assert b is not None
        try:
            workflow_id = self.sh.api.submit(
                b.workflow_source,
                self.inputs,
                self.options,
                b.dependencies,
                workflow_filename=b.workflow.rsplit('/', 1)[-1],
            )
        except TransientError as e:
            self._error(now, e)
            return
        except CromwellRequestError as e:
            self.finish('NotSubmitted', EXIT_USAGE, error=f'Cromwell rejected the submission: {e}')
            return
        self.result.workflow_id = workflow_id
        self.result.reattached = False
        self.result.submitted_ar_guid = self.spec.ar_guid
        self.log(logging.INFO, 'submitted workflow %s (ar-guid %s)', workflow_id, self.spec.ar_guid)
        if self.sh.store is not None and not self._write_record(workflow_id):
            return  # lost a race: now attached to the other process's workflow (or resubmitting)
        self._start_watch(now, workflow_id)

    def _write_record(self, workflow_id: str) -> bool:
        store = self.sh.store
        assert store is not None
        record = ReattachRecord(workflow_id, self.spec.name, now_iso(), self.spec.ar_guid)
        try:
            store.write(self.key, record, replace_generation=self.record_generation)
            return True
        except PreconditionFailed:
            self.races += 1
            self.log(
                logging.WARNING,
                'another process wrote re-attach record %s first; aborting duplicate %s',
                self.key[:12],
                workflow_id,
            )
            self._abort_quietly(workflow_id)
            self.result.workflow_id = None
            if self.races > MAX_RECORD_RACES:
                self.finish('NotSubmitted', EXIT_TRANSIENT, error='kept losing the re-attach record race')
                return False
            self.phase, self.next_at = Phase.LOOKUP, self.sh.ctx.clock()
            return False
        except TransientError as e:
            self.log(logging.WARNING, 'could not write re-attach record (%s); continuing without it', e)
            return True

    def _abort_quietly(self, workflow_id: str, attempts: int = 3) -> bool:
        for _ in range(attempts):
            try:
                self.sh.api.abort(workflow_id)
                self.log(logging.WARNING, 'abort requested for %s', workflow_id)
                return True
            except CromwellRequestError as e:  # already terminal (403) or unknown (404)
                self.log(logging.INFO, 'abort %s: %s', workflow_id, e)
                return True
            except TransientError as e:
                self.log(logging.WARNING, 'abort %s failed: %s', workflow_id, e)
        return False

    def _wait_aborted(self, now: float) -> None:
        try:
            status = self.sh.api.status(self.wid)
        except TransientError as e:
            self._error(now, e, interval=self.sh.ctx.abort_poll_seconds)
            return
        except CromwellRequestError:
            status = None
        self.errors = 0
        if status == 'Succeeded':
            self.result.reattached = True
            self._terminal('Succeeded')
            return
        if status in (None, 'Aborted', 'Failed') or now >= self.deadline:
            if status not in (None, 'Aborted', 'Failed'):
                self.log(
                    logging.WARNING,
                    '%s still %s after %ss; resubmitting anyway',
                    self.wid,
                    status,
                    self.sh.ctx.abort_confirm_seconds,
                )
            self.result.workflow_id = None
            self.phase, self.next_at = Phase.SUBMIT, now
            return
        self.next_at = now + self.sh.ctx.abort_poll_seconds

    def _watch(self, now: float) -> None:
        assert self.poll is not None
        limit = self.policy.time_limit_seconds
        if limit and now - self.limit_start >= limit:
            self.log(logging.WARNING, 'exceeded the time limit (%ss): aborting %s', limit, self.wid)
            self.phase = Phase.ABORT
            self.abort_sent = False
            self.deadline = now + self.sh.ctx.abort_confirm_seconds
            self.next_at = now
            return
        interval = self.poll.next_interval(now)
        try:
            status = self.sh.api.status(self.wid)
        except (TransientError, CromwellRequestError) as e:
            self._error(now, e, interval=interval)
            return
        self.errors = 0
        if status in TERMINAL_STATUSES:
            self._terminal(status)
            return
        self.log(logging.DEBUG, 'status %s (next poll in %ss)', status, interval)
        self.next_at = now + interval

    def _abort_for_timeout(self, now: float) -> None:
        if not self.abort_sent:
            try:
                self.sh.api.abort(self.wid)
                self.abort_sent = True
            except CromwellRequestError as e:  # already terminal: see what it ended as
                self.log(logging.INFO, 'abort: %s', e)
                self.abort_sent = True
            except TransientError as e:
                self._error(now, e, interval=self.sh.ctx.abort_poll_seconds)
                return
        try:
            status = self.sh.api.status(self.wid)
        except (TransientError, CromwellRequestError) as e:
            self._error(now, e, interval=self.sh.ctx.abort_poll_seconds)
            return
        self.errors = 0
        if status in TERMINAL_STATUSES:
            self._terminal(status, timed_out=True)
            return
        if now >= self.deadline:
            self._release()
            self.finish('TimedOut', EXIT_TIMEOUT, error=f'time limit exceeded; abort not confirmed (still {status})')
            return
        self.next_at = now + self.sh.ctx.abort_poll_seconds

    def _terminal(self, status: str, *, timed_out: bool = False) -> None:
        self._release()
        failures = self._report()
        if status == 'Succeeded':
            self.result.status = 'Succeeded'
            self.result.failures = failures
            self.phase = Phase.COLLECTING
            self.next_at = self.sh.ctx.clock() + 5
            self.future = self.sh.collector.submit(self._collect_job)
            self.future.add_done_callback(self._collected)
        elif status == 'Failed':
            self.finish('Failed', EXIT_FAILED, failures=failures, error=(failures or ['workflow failed'])[0])
        elif timed_out:
            limit = self.policy.time_limit_seconds
            self.finish('TimedOut', EXIT_TIMEOUT, failures=failures, error=f'time limit of {limit}s exceeded; aborted')
        else:
            self.finish('Aborted', EXIT_FAILED, failures=failures, error='workflow was aborted outside this process')

    def _report(self) -> list[str]:
        """Print WorkflowMetadataModel.display() from a trimmed metadata fetch. Never fails the run."""
        try:
            from cpg_utils.cromwell_model import WorkflowMetadataModel  # noqa: PLC0415

            metadata = self.sh.api.metadata(self.wid, DISPLAY_METADATA_KEYS)
            failures = extract_failures(metadata)
            try:
                metadata.setdefault('calls', {})
                text = WorkflowMetadataModel.parse(metadata).display(monochrome=not sys.stderr.isatty())
                self.log(logging.INFO, 'workflow %s concluded:%s', self.wid, text)
            except Exception as e:  # noqa: BLE001
                self.log(logging.WARNING, 'could not display metadata: %s', e)
            return failures
        except Exception as e:  # noqa: BLE001
            self.log(logging.WARNING, 'failed to collect run metadata: %s', e)
            return []

    # ---- collection (background thread) -------------------------------------------------

    def _collect_job(self) -> dict[str, Any]:
        attempts = 0
        while True:
            if self.cancel.is_set():
                raise Cancelled('collection cancelled')
            try:
                outputs = self.sh.api.outputs(self.wid)
                break
            except (TransientError, CromwellNotFound) as e:
                attempts += 1
                if attempts >= self.policy.max_consecutive_errors:
                    raise TransientError(f'could not fetch outputs of {self.wid}: {e}') from e
                self.log(logging.WARNING, 'fetching outputs failed (%d): %s', attempts, e)
                self.cancel.wait(min(60.0, 2.0**attempts))
            except CromwellRequestError as e:
                raise TransientError(f'could not fetch outputs of {self.wid}: {e}') from e
        self.log(logging.INFO, 'collecting %d output(s)', len(self.spec.collect or {}))
        return collect_outputs(
            self.spec, outputs, self.sh.storage, cancel=self.cancel, threads=self.sh.ctx.copy_threads
        )

    def _collected(self, _future: Future) -> None:
        self.next_at = 0.0
        self.sh.wake.set()

    def _collecting(self, now: float) -> None:
        future = self.future
        assert future is not None
        if not future.done():
            self.next_at = now + 5
            return
        try:
            outputs = future.result()
        except CromwellCliError as e:
            self.finish('Succeeded', e.exit_code, error=f'output collection failed: {e}', failures=self.result.failures)
            return
        except Exception as e:  # noqa: BLE001
            self.finish('Succeeded', EXIT_FAILED, error=f'output collection failed: {e!r}')
            return
        self.finish('Succeeded', EXIT_OK, outputs=outputs)

    # ---- signals -------------------------------------------------------------------------

    def _detach_status(self, workflow_id: str) -> tuple[Status, str]:
        """One status check when detaching, so the result says what the workflow is really doing."""
        try:
            status = self.sh.api.status(workflow_id)
        except (TransientError, CromwellRequestError) as e:
            self.log(logging.WARNING, 'detaching: could not check the status of %s: %s', workflow_id, e)
            return 'Unreachable', f'detached on signal; could not check the workflow status: {e}'
        self.log(logging.WARNING, 'detaching: leaving %s (%s)', workflow_id, status)
        if status in TERMINAL_STATUSES:
            return status, f'detached on signal; the workflow had already finished ({status})'  # type: ignore[return-value]
        return status, 'detached on signal (workflow left running)'  # type: ignore[return-value]

    def on_signal(self, signum: int, abort: bool) -> None:
        code = 128 + signum
        if self.phase in (Phase.WATCH, Phase.ABORT, Phase.WAIT_ABORTED) and self.result.workflow_id:
            if abort:
                ok = self._abort_quietly(self.result.workflow_id)
                status: Status = 'Aborted'
                error = 'aborted on signal' if ok else 'signal received; abort request failed'
            else:
                status, error = self._detach_status(self.result.workflow_id)
            self.finish(status, code, error=error, notify=False)
        elif self.phase is Phase.COLLECTING:
            self.cancel.set()
            self.finish('Succeeded', code, error='output collection interrupted by signal', notify=False)
        else:
            self.finish('NotSubmitted', code, error='not submitted: signal received', notify=False)
        self.sh.on_finish(self, call_back=False)  # type: ignore[call-arg]


def _summary(results: Sequence[WorkflowResult]) -> str:
    rows = [('name', 'workflow id', 'status', 'exit', 'detail')]
    for r in results:
        detail = (r.error or (r.failures[0] if r.failures else '')).splitlines()
        rows.append((r.name, r.workflow_id or '-', r.status, str(r.exit_code), detail[0][:120] if detail else ''))
    widths = [max(len(row[i]) for row in rows) for i in range(4)]
    return '\n'.join(
        '  '.join(c.ljust(w) for c, w in zip(row[:4], widths, strict=True)) + '  ' + row[4] for row in rows
    )


def run_many(
    specs: Sequence[WorkflowSpec],
    *,
    reattach_root: str | None = None,
    submit_interval: float = 1.0,
    max_in_flight: int | None = None,
    results_dir: str | None = None,
    on_result: Callable[[WorkflowResult], None] | None = None,
    abort_on_signal: bool = True,
    context: RunContext | None = None,
) -> list[WorkflowResult]:
    """
    Submit (or re-attach to), watch and collect every spec. One failing workflow never stops the
    others. Each result is final -> collected -> on_result -> written to results_dir, in that order.
    """
    names = [s.name for s in specs]
    if not specs:
        raise SpecError('no specs given')
    if len(set(names)) != len(names):
        raise SpecError(f'spec names must be unique: {sorted({n for n in names if names.count(n) > 1})}')
    if max_in_flight is not None and max_in_flight < 1:
        raise SpecError('max_in_flight must be >= 1')

    ctx = (context or RunContext()).resolved()
    storage = ctx.storage
    assert storage is not None
    wake = threading.Event()
    store = ReattachStore(reattach_root, storage) if reattach_root else None

    def on_finish(runner: WorkflowRunner, call_back: bool = True) -> None:
        result = runner.result
        if call_back and on_result is not None:
            try:
                on_result(result)
            except Exception as e:
                logger.exception('[%s] on_result callback failed', result.name)
                result.exit_code = EXIT_FAILED if result.exit_code == EXIT_OK else result.exit_code
                result.error = result.error or f'on_result callback failed: {e!r}'
        if results_dir:
            uri = join(results_dir, result_filename(result.name))
            try:
                storage.write(uri, result.to_json(indent=2))
            except Exception as e:  # noqa: BLE001
                logger.error('[%s] could not write %s: %s', result.name, uri, e)
                if result.exit_code == EXIT_OK:
                    result.exit_code = EXIT_TRANSIENT
                    result.error = f'could not write result file: {e}'

    with ThreadPoolExecutor(max_workers=max(1, ctx.collect_workers), thread_name_prefix='collect') as collector:
        shared = _Shared(ctx, store, SubmitGate(submit_interval, max_in_flight), wake, collector, on_finish)
        runners = [WorkflowRunner(s, shared) for s in specs]
        logger.info('running %d workflow(s); re-attach root: %s', len(runners), reattach_root or 'none')
        # always trap: abort_on_signal only chooses between aborting and detaching, and either way
        # the loop must stop cleanly so results are recorded and the exit code is 128 + signum
        with SignalTrap(wake) as trap:
            signum = Scheduler(ctx.clock, wake).run(runners, trap, abort_on_signal=abort_on_signal)
        if signum is not None:
            collector.shutdown(wait=True, cancel_futures=True)

    results = [r.result for r in runners]
    if len(results) > 1:
        logger.info('summary:\n%s', _summary(results))
    return results


def run(
    spec: WorkflowSpec,
    *,
    reattach_root: str | None = None,
    abort_on_signal: bool = True,
    context: RunContext | None = None,
) -> WorkflowResult:
    """Run one workflow: `run_many([spec])` with single-workflow exit semantics."""
    return run_many(
        [spec], reattach_root=reattach_root, submit_interval=0, abort_on_signal=abort_on_signal, context=context
    )[0]
