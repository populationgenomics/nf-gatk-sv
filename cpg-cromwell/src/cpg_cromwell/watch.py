"""
Scheduling for one or many workflows (design §7.5, §7.6): polling curve, jitter, signal trap,
submission pacing and the event loop. The per-workflow state machine lives in run.py.
"""

import logging
import math
import random
import signal
import threading
import time
from collections.abc import Callable, Iterable
from typing import Protocol, Self

from cpg_cromwell.spec import PollPolicy

logger = logging.getLogger(__name__)


def poll_interval(elapsed: float, min_seconds: float, max_seconds: float, ramp_seconds: float) -> float:
    """Same curve as cpg_utils.cromwell.watch_workflow._get_wait_interval."""
    factor = elapsed / ramp_seconds
    if factor > 1:
        return max_seconds
    return max(min_seconds, (1 - math.cos(math.pi * factor)) * max_seconds // 2)


def draw_bounds(policy: PollPolicy, rng: random.Random) -> tuple[float, float]:
    """Jitter is drawn once per workflow: min=randint(min, 2min), max=randint(max, 2max)."""
    if not policy.jitter:
        return policy.min_seconds, policy.max_seconds

    def draw(x: float) -> float:
        if float(x).is_integer():
            return rng.randint(int(x), int(2 * x))
        return rng.uniform(x, 2 * x)

    return draw(policy.min_seconds), draw(policy.max_seconds)


class PollClock:
    """A workflow's polling curve, starting from its own submit/attach time."""

    def __init__(self, policy: PollPolicy, start: float, rng: random.Random) -> None:
        self.min_seconds, self.max_seconds = draw_bounds(policy, rng)
        self.ramp_seconds = policy.ramp_seconds
        self.start = start

    def next_interval(self, now: float) -> float:
        return poll_interval(now - self.start, self.min_seconds, self.max_seconds, self.ramp_seconds)


class SignalTrap:
    """
    Turns SIGTERM/SIGINT into a flag plus a wake-up, so the loop can abort (or detach from) what
    it started and exit 143/130. Only installs handlers on the main thread; restores them on exit.
    """

    SIGNALS = (signal.SIGTERM, signal.SIGINT)

    def __init__(self, wake: threading.Event) -> None:
        self.wake = wake
        self.signum: int | None = None
        self._previous: dict[int, object] = {}

    def _handle(self, signum: int, _frame: object) -> None:
        if self.signum is None:
            self.signum = signum
            logger.warning('received %s: stopping', signal.Signals(signum).name)
        self.wake.set()

    def __enter__(self) -> Self:
        if threading.current_thread() is threading.main_thread():
            for s in self.SIGNALS:
                self._previous[s] = signal.signal(s, self._handle)
        else:
            logger.warning('not on the main thread: SIGTERM/SIGINT will not stop the run cleanly')
        return self

    def __exit__(self, *exc: object) -> None:
        for s, handler in self._previous.items():
            signal.signal(s, handler)  # type: ignore[arg-type]

    @property
    def triggered(self) -> bool:
        return self.signum is not None


class SubmitGate:
    """One submission every `interval` seconds, at most `max_in_flight` workflows running."""

    def __init__(self, interval: float, max_in_flight: int | None) -> None:
        self.interval = interval
        self.max_in_flight = max_in_flight
        self.next_at = 0.0
        self.in_flight = 0

    def wait_until(self, now: float) -> float | None:
        """None if a submission may happen now, else when to try again."""
        if self.max_in_flight is not None and self.in_flight >= self.max_in_flight:
            return now + max(self.interval, 0.25)
        if now < self.next_at:
            return self.next_at
        return None

    def submitted(self, now: float) -> None:
        self.next_at = now + self.interval


class Task(Protocol):
    next_at: float

    @property
    def done(self) -> bool: ...

    def step(self, now: float) -> None: ...

    def on_signal(self, signum: int, abort: bool) -> None: ...


class Scheduler:
    """
    Single-threaded loop over tasks. Each iteration steps every task that is due, then sleeps
    until the earliest `next_at` or until woken (signal, or a background collection finished).
    """

    def __init__(
        self,
        clock: Callable[[], float] = time.monotonic,
        wake: threading.Event | None = None,
    ) -> None:
        self.clock = clock
        self.wake = wake or threading.Event()

    def run(self, tasks: Iterable[Task], trap: SignalTrap, *, abort_on_signal: bool) -> int | None:
        """Returns the signal number if one stopped the loop."""
        active = list(tasks)
        while True:
            active = [t for t in active if not t.done]
            if trap.triggered:
                for t in active:
                    t.on_signal(trap.signum, abort_on_signal)  # type: ignore[arg-type]
                return trap.signum
            if not active:
                return None
            now = self.clock()
            due = sorted((t for t in active if t.next_at <= now), key=lambda t: t.next_at)
            for t in due:
                if trap.triggered:
                    break
                t.step(self.clock())
            if due:
                continue
            timeout = min(t.next_at for t in active) - now
            if timeout > 0:
                self.wake.wait(timeout)
                self.wake.clear()
