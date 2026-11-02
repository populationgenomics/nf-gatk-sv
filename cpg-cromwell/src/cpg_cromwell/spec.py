"""Core types: WorkflowSpec and friends (design §5), with JSON round-trip and label validation (§7.9)."""

import dataclasses
import json
import os
import re
from collections.abc import Mapping
from dataclasses import dataclass, field
from typing import Any, Literal, Self

from cpg_cromwell.errors import EXIT_OK, SpecError

AR_GUID_LABEL = 'ar-guid'
COMPUTE_CATEGORY_LABEL = 'compute-category'
RESERVED_LABELS = (AR_GUID_LABEL, COMPUTE_CATEGORY_LABEL)
MAX_LABELS = 64
_LABEL_KEY_RE = re.compile(r'^[a-z][a-z0-9_-]{0,62}$')
_LABEL_VALUE_RE = re.compile(r'^[a-z0-9_-]{0,63}$')

# The last four are Cromwell's live statuses: only seen when the run detached on a signal
# (--no-abort-on-exit) and the workflow was still going when it was checked.
Status = Literal[
    'Succeeded',
    'Failed',
    'Aborted',
    'TimedOut',
    'Unreachable',
    'NotSubmitted',
    'Submitted',
    'Running',
    'On Hold',
    'Aborting',
]


def _check_fields(cls: type, data: Mapping[str, Any], what: str) -> None:
    if not isinstance(data, Mapping):
        raise SpecError(f'{what} must be a JSON object, got {type(data).__name__}')
    known = {f.name for f in dataclasses.fields(cls)}
    unknown = set(data) - known
    if unknown:
        raise SpecError(f'{what}: unknown field(s) {sorted(unknown)}')
    required = {
        f.name
        for f in dataclasses.fields(cls)
        if f.default is dataclasses.MISSING and f.default_factory is dataclasses.MISSING
    }
    missing = required - set(data)
    if missing:
        raise SpecError(f'{what}: missing field(s) {sorted(missing)}')


@dataclass(frozen=True)
class WorkflowSource:
    bundle: str  # gs://.../bundles/<repo>/<commit>/  (made by `cpg-cromwell bundle`)
    workflow: str  # path inside the bundle, e.g. "GatherSampleEvidence.wdl"

    def __post_init__(self) -> None:
        if not self.bundle:
            raise SpecError('source.bundle must be set')
        parts = self.workflow.split('/')
        if not self.workflow or self.workflow.startswith('/') or '..' in parts or '' in parts:
            raise SpecError(f'source.workflow must be a relative path inside the bundle: {self.workflow!r}')

    @property
    def bundle_root(self) -> str:
        return self.bundle.rstrip('/') + '/'

    @property
    def workflow_uri(self) -> str:
        return self.bundle_root + self.workflow

    @classmethod
    def from_dict(cls, data: Mapping[str, Any]) -> Self:
        _check_fields(cls, data, 'source')
        return cls(**data)


@dataclass(frozen=True)
class PollPolicy:
    min_seconds: float = 5
    max_seconds: float = 60
    ramp_seconds: float = 1200  # cosine ramp from min to max, same curve as watch_workflow
    jitter: bool = False  # min=randint(min, 2min), max=randint(max, 2max), drawn once per workflow
    max_consecutive_errors: int = 25
    time_limit_seconds: float | None = None

    def __post_init__(self) -> None:
        if self.min_seconds < 0 or self.max_seconds < self.min_seconds:
            raise SpecError(f'poll: need 0 <= min_seconds <= max_seconds, got {self.min_seconds}/{self.max_seconds}')
        if self.ramp_seconds <= 0:
            raise SpecError('poll.ramp_seconds must be > 0')
        if self.max_consecutive_errors < 1:
            raise SpecError('poll.max_consecutive_errors must be >= 1')
        if self.time_limit_seconds is not None and self.time_limit_seconds <= 0:
            raise SpecError('poll.time_limit_seconds must be > 0 (or null)')

    @classmethod
    def from_dict(cls, data: Mapping[str, Any]) -> Self:
        _check_fields(cls, data, 'poll')
        return cls(**data)


def serialise_input_value(value: Any) -> Any:
    """
    Serialise one top-level input value the way `add_gatk_sv_jobs` does:
    paths -> str, list/set -> list[str] (every element str()'d, as the original does).
    Tuples become lists the same way. Sets are sorted so the result (and the re-attach key)
    is deterministic. Idempotent, so a spec survives any number of JSON round-trips.
    """
    if isinstance(value, os.PathLike):  # pathlib and cloudpathlib; str() never touches the filesystem
        return str(value)
    if isinstance(value, set | frozenset):
        return sorted(str(v) for v in value)
    if isinstance(value, list | tuple):
        return [str(v) for v in value]
    return value


def serialise_inputs(inputs: Mapping[str, Any]) -> dict[str, Any]:
    if not isinstance(inputs, Mapping):
        raise SpecError('inputs must be a mapping')
    out = {}
    for key, value in inputs.items():
        if not isinstance(key, str) or not key:
            raise SpecError(f'input keys must be non-empty strings, got {key!r}')
        out[key] = serialise_input_value(value)
    try:
        json.dumps(out)
    except (TypeError, ValueError) as e:
        raise SpecError(f'inputs are not JSON-serialisable: {e}') from e
    return out


def validate_labels(labels: Mapping[str, str], ar_guid: str | None) -> None:
    """Validation rules from design §7.9. Values are never normalised for the caller."""
    for reserved in RESERVED_LABELS:
        if reserved in labels:
            hint = 'use WorkflowSpec.ar_guid' if reserved == AR_GUID_LABEL else 'it is always set to "cromwell"'
            raise SpecError(f'labels must not contain {reserved!r} ({hint})')
    every = dict(labels)
    every[COMPUTE_CATEGORY_LABEL] = 'cromwell'
    if ar_guid is not None:
        every[AR_GUID_LABEL] = ar_guid
    for key, value in every.items():
        if not isinstance(key, str) or not _LABEL_KEY_RE.match(key):
            raise SpecError(
                f'invalid label key {key!r}: lowercase letters, digits, "_" and "-", '
                'max 63 characters, starting with a letter',
            )
        if not isinstance(value, str) or not _LABEL_VALUE_RE.match(value):
            raise SpecError(
                f'invalid value for label {key!r}: {value!r}: lowercase letters, digits, "_" and "-", '
                'max 63 characters (values are not lowercased or truncated for you)',
            )
    if len(every) > MAX_LABELS:
        raise SpecError(f'too many labels: {len(every)} > {MAX_LABELS} (including compute-category and ar-guid)')


def _normalise_collect(collect: Mapping[str, Any] | None) -> dict[str, str | list[str]] | None:
    if collect is None:
        return None
    if not isinstance(collect, Mapping):
        raise SpecError('collect must be a mapping of output key -> destination(s)')
    out: dict[str, str | list[str]] = {}
    for key, dest in collect.items():
        if not isinstance(key, str) or not key:
            raise SpecError(f'collect keys must be non-empty strings, got {key!r}')
        if isinstance(dest, os.PathLike | str):
            out[key] = str(dest)
        elif isinstance(dest, list | tuple):
            if not dest or not all(isinstance(d, os.PathLike | str) and str(d) for d in dest):
                raise SpecError(f'collect[{key!r}] must be a non-empty list of paths')
            out[key] = [str(d) for d in dest]
        else:
            raise SpecError(f'collect[{key!r}] must be a path or a list of paths')
        if isinstance(out[key], str) and not out[key]:
            raise SpecError(f'collect[{key!r}] is empty')
    return out


@dataclass(frozen=True)
class WorkflowSpec:
    name: str  # stable id for logs/results (e.g. "GatherSampleEvidence/CPG123")
    source: WorkflowSource
    inputs: dict[str, Any]  # unprefixed; normalised on construction (see serialise_inputs)
    input_prefix: str | None  # "GatherSampleEvidence" -> keys become "GatherSampleEvidence.<k>"
    dataset: str  # selects the Cromwell SA secret and default log/tmp paths
    access_level: str
    output_prefix: str  # cromwell_logs/<output_prefix>; final_workflow_outputs_dir
    google_project: str | None = None  # default: config workflow.dataset_gcp_project
    ar_guid: str | None = None  # becomes google_labels['ar-guid'] (§7.9)
    labels: dict[str, str] = field(default_factory=dict)  # must not contain 'ar-guid'
    copy_outputs: bool = False
    collect: dict[str, str | list[str]] | None = None  # output key -> destination(s)
    poll: PollPolicy = field(default_factory=PollPolicy)

    def __post_init__(self) -> None:
        for attr in ('name', 'dataset', 'access_level', 'output_prefix'):
            if not isinstance(getattr(self, attr), str) or not getattr(self, attr):
                raise SpecError(f'{attr} must be a non-empty string')
        if not isinstance(self.source, WorkflowSource):
            raise SpecError('source must be a WorkflowSource')
        if not isinstance(self.poll, PollPolicy):
            raise SpecError('poll must be a PollPolicy')
        if self.input_prefix is not None and (not isinstance(self.input_prefix, str) or not self.input_prefix):
            raise SpecError('input_prefix must be a non-empty string or null')
        if not isinstance(self.labels, Mapping):
            raise SpecError('labels must be a mapping')
        object.__setattr__(self, 'inputs', serialise_inputs(self.inputs))
        object.__setattr__(self, 'labels', dict(self.labels))
        object.__setattr__(self, 'collect', _normalise_collect(self.collect))
        validate_labels(self.labels, self.ar_guid)

    def prefixed_inputs(self) -> dict[str, Any]:
        if not self.input_prefix:
            return dict(self.inputs)
        return {f'{self.input_prefix}.{k}': v for k, v in self.inputs.items()}

    def output_key(self, key: str) -> str:
        return f'{self.input_prefix}.{key}' if self.input_prefix else key

    def to_dict(self) -> dict[str, Any]:
        return dataclasses.asdict(self)

    def to_json(self, **kwargs: Any) -> str:
        return json.dumps(self.to_dict(), **kwargs)

    @classmethod
    def from_dict(cls, data: Mapping[str, Any]) -> Self:
        _check_fields(cls, data, 'spec')
        data = dict(data)
        data['source'] = WorkflowSource.from_dict(data['source'])
        data['poll'] = PollPolicy.from_dict(data.get('poll') or {})
        if data.get('labels') is None:
            data['labels'] = {}
        try:
            return cls(**data)
        except TypeError as e:
            raise SpecError(f'spec: {e}') from e

    @classmethod
    def from_json(cls, text: str) -> Self:
        try:
            data = json.loads(text)
        except json.JSONDecodeError as e:
            raise SpecError(f'spec is not valid JSON: {e}') from e
        return cls.from_dict(data)


@dataclass
class WorkflowResult:
    name: str
    workflow_id: str | None
    status: Status
    reattached: bool = False
    submitted_ar_guid: str | None = None  # GUID of the launch that submitted it
    outputs: dict[str, Any] | None = None  # {key: destination or raw value} once collected
    failures: list[str] = field(default_factory=list)  # short messages from the metadata
    # additions to the design's fields: the per-workflow exit code (§6.1) and why it isn't 0
    exit_code: int = EXIT_OK
    error: str | None = None

    @property
    def ok(self) -> bool:
        return self.exit_code == EXIT_OK

    def to_dict(self) -> dict[str, Any]:
        return dataclasses.asdict(self)

    def to_json(self, **kwargs: Any) -> str:
        return json.dumps(self.to_dict(), **kwargs)

    @classmethod
    def from_dict(cls, data: Mapping[str, Any]) -> Self:
        return cls(**data)


def read_specs_jsonl(text: str) -> list[WorkflowSpec]:
    specs = []
    for lineno, line in enumerate(text.splitlines(), start=1):
        if not line.strip():
            continue
        try:
            specs.append(WorkflowSpec.from_json(line))
        except SpecError as e:
            raise SpecError(f'line {lineno}: {e}') from e
    if not specs:
        raise SpecError('no specs given')
    names = [s.name for s in specs]
    dupes = sorted({n for n in names if names.count(n) > 1})
    if dupes:
        raise SpecError(f'spec names must be unique; duplicated: {dupes}')
    return specs
