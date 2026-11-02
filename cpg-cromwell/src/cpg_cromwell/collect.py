"""
Strict output collection (design §7.7).

For each `collect` key, `<input_prefix>.<key>` must be present and non-null in the Cromwell
outputs. List destinations need an array of exactly the same length. gs:// values are copied
server-side; other values are passed through into outputs.json unchanged.
"""

import logging
import threading
from concurrent.futures import ThreadPoolExecutor
from typing import Any

from cpg_cromwell.errors import Cancelled, CollectError, CromwellCliError, TransientError
from cpg_cromwell.spec import WorkflowSpec
from cpg_cromwell.storage import Storage

logger = logging.getLogger(__name__)

DEFAULT_COPY_THREADS = 8


def plan_collection(spec: WorkflowSpec, outputs: dict[str, Any]) -> tuple[dict[str, Any], list[tuple[str, str]]]:
    """
    Validate outputs against spec.collect. Returns (outputs.json content, [(src, dst) copies]).
    Raises CollectError on a missing/null key or length mismatch. Pure: no I/O.
    """
    result: dict[str, Any] = {}
    copies: list[tuple[str, str]] = []

    def one(label: str, value: Any, dest: str) -> Any:
        if value is None:
            raise CollectError(f'output {label} is null')
        if isinstance(value, str) and value.startswith('gs://'):
            copies.append((value, dest))
            return dest
        if isinstance(value, list | dict):
            raise CollectError(f'output {label} is a {type(value).__name__}, expected a single value for {dest}')
        return value

    for key, dest in (spec.collect or {}).items():
        full = spec.output_key(key)
        if full not in outputs:
            raise CollectError(f'output {full} is missing from the workflow outputs')
        value = outputs[full]
        if value is None:
            raise CollectError(f'output {full} is null')
        if isinstance(dest, list):
            if not isinstance(value, list):
                raise CollectError(f'output {full} is not an array, but {len(dest)} destinations were given')
            if len(value) != len(dest):
                raise CollectError(f'output {full} has {len(value)} elements, expected {len(dest)}')
            result[key] = [one(f'{full}[{i}]', v, d) for i, (v, d) in enumerate(zip(value, dest, strict=True))]
        else:
            result[key] = one(full, value, dest)
    return result, copies


def collect_outputs(
    spec: WorkflowSpec,
    outputs: dict[str, Any],
    storage: Storage,
    *,
    cancel: threading.Event | None = None,
    threads: int = DEFAULT_COPY_THREADS,
) -> dict[str, Any]:
    """Copy everything, then return {key: destination or value}. Writing outputs.json is the caller's job."""
    if spec.collect is None:
        return dict(outputs)
    result, copies = plan_collection(spec, outputs)
    if not copies:
        return result

    def copy(pair: tuple[str, str]) -> CromwellCliError | None:
        src, dst = pair
        try:
            if storage.copy(src, dst, cancel=cancel):
                logger.info('[%s] copied %s -> %s', spec.name, src, dst)
            return None
        except CromwellCliError as e:
            return e

    with ThreadPoolExecutor(max_workers=max(1, threads)) as pool:
        errors = [e for e in pool.map(copy, copies) if e is not None]
    if errors:
        for e in errors:
            logger.error('[%s] %s', spec.name, e)
        # a non-transient failure wins: retrying would hit it again
        for kind in (Cancelled, CollectError, TransientError):
            for e in errors:
                if isinstance(e, kind):
                    raise e
        raise errors[0]
    return result
