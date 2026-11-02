"""Exceptions and the exit-code contract (design §6.1)."""

EXIT_OK = 0
EXIT_FAILED = 1
EXIT_USAGE = 2
EXIT_TRANSIENT = 75
EXIT_TIMEOUT = 124
EXIT_SIGINT = 130
EXIT_SIGTERM = 143

# run-many: the highest-priority code among failures wins. A signal always wins.
# 2 is not in the design's list; it sits after 75 because a transient failure in one
# chunk member should still get the whole chunk retried (re-attach keeps that cheap).
EXIT_PRIORITY = (EXIT_SIGTERM, EXIT_SIGINT, EXIT_TRANSIENT, EXIT_USAGE, EXIT_FAILED, EXIT_TIMEOUT)


class CromwellCliError(Exception):
    """Base class. `exit_code` is what the CLI exits with when this escapes."""

    exit_code = EXIT_FAILED


class SpecError(CromwellCliError):
    """Bad spec, labels, bundle, missing secret or config: don't retry."""

    exit_code = EXIT_USAGE


class BundleError(SpecError):
    """Bundle missing, malformed, or not verified on main."""


class TransientError(CromwellCliError):
    """Cromwell/GCS/auth unavailable: the orchestrator should retry."""

    exit_code = EXIT_TRANSIENT


class CollectError(CromwellCliError):
    """Strict output collection failed in a non-transient way."""

    exit_code = EXIT_FAILED


class Cancelled(CromwellCliError):
    """Work stopped because the process received a signal."""


def combine_exit_codes(codes: list[int]) -> int:
    """Pick the overall exit code for a set of per-workflow exit codes."""
    failing = {c for c in codes if c != EXIT_OK}
    if not failing:
        return EXIT_OK
    for code in EXIT_PRIORITY:
        if code in failing:
            return code
    return max(failing)
