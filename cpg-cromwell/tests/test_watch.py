import math
import random

from cpg_cromwell.errors import combine_exit_codes
from cpg_cromwell.spec import PollPolicy
from cpg_cromwell.watch import SubmitGate, draw_bounds, poll_interval


def old_get_wait_interval(elapsed: float, lo: int, hi: int, ramp: int) -> int:
    """Verbatim logic of cpg_utils.cromwell.watch_workflow._get_wait_interval."""
    factor = elapsed / ramp
    if factor > 1:
        return hi
    return max(lo, int((1 - math.cos(math.pi * factor)) * hi // 2))


def test_curve_matches_watch_workflow() -> None:
    for lo, hi in ((5, 60), (30, 140), (200, 2000)):
        for t in range(0, 1500, 7):
            assert poll_interval(t, lo, hi, 1200) == old_get_wait_interval(t, lo, hi, 1200)


def test_curve_shape() -> None:
    assert poll_interval(0, 5, 60, 1200) == 5
    assert poll_interval(1201, 5, 60, 1200) == 60
    values = [poll_interval(t, 5, 60, 1200) for t in range(0, 1200, 10)]
    assert values == sorted(values)


def test_jitter_bounds() -> None:
    rng = random.Random(1)
    policy = PollPolicy(min_seconds=40, max_seconds=400, jitter=True)
    for _ in range(500):
        lo, hi = draw_bounds(policy, rng)
        assert 40 <= lo <= 80
        assert 400 <= hi <= 800
        assert isinstance(lo, int)
    assert draw_bounds(PollPolicy(min_seconds=40, max_seconds=400), rng) == (40, 400)


def test_submit_gate() -> None:
    gate = SubmitGate(interval=1.0, max_in_flight=2)
    assert gate.wait_until(0) is None
    gate.submitted(0)
    assert gate.wait_until(0.5) == 1.0
    assert gate.wait_until(1.0) is None
    gate.in_flight = 2
    assert gate.wait_until(5) is not None


def test_combine_exit_codes() -> None:
    assert combine_exit_codes([0, 0]) == 0
    assert combine_exit_codes([0, 124, 1]) == 1
    assert combine_exit_codes([1, 75, 124]) == 75
    assert combine_exit_codes([124]) == 124
    assert combine_exit_codes([2, 1]) == 2
    assert combine_exit_codes([75, 143]) == 143
