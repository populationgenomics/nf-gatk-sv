"""
Idempotent relaunch (design §7.4).

Key = sha256(canonical JSON of repo, commit, workflow path, prefixed inputs, options minus
the secret and google_labels). Record = `<root>/<key>.json`, written with GCS generation
preconditions so two racing processes end up attached to the same workflow.
"""

import datetime
import enum
import hashlib
import json
from dataclasses import asdict, dataclass
from typing import Any

from cpg_cromwell.api import ABORTING_STATUS, ACTIVE_STATUSES
from cpg_cromwell.options import options_for_key
from cpg_cromwell.storage import PreconditionFailed, Storage, join


def canonical_json(value: Any) -> str:
    return json.dumps(value, sort_keys=True, separators=(',', ':'), ensure_ascii=False)


def reattach_key(
    repo: str, commit: str, workflow: str, prefixed_inputs: dict[str, Any], options: dict[str, Any]
) -> str:
    payload = {
        'repo': repo,
        'commit': commit,
        'workflow': workflow,
        'inputs': prefixed_inputs,
        'options': options_for_key(options),
    }
    return hashlib.sha256(canonical_json(payload).encode()).hexdigest()


class Action(enum.Enum):
    SUBMIT = 'submit'  # no record / 404 / Failed / Aborted: submit and write (or replace) the record
    ATTACH = 'attach'  # Submitted / Running / On Hold: watch it
    WAIT_ABORTED = 'wait_aborted'  # Aborting: wait until Aborted, then resubmit
    COLLECT = 'collect'  # Succeeded: go straight to collection


def action_for(status: str | None) -> Action:
    """The §7.4 state machine. `None` means no record, or Cromwell returned 404 for it."""
    if status is None or status in ('Failed', 'Aborted'):
        return Action.SUBMIT
    if status == 'Succeeded':
        return Action.COLLECT
    if status == ABORTING_STATUS:
        return Action.WAIT_ABORTED
    if status in ACTIVE_STATUSES:
        return Action.ATTACH
    # unknown, non-terminal status: safer to watch it than to submit a duplicate
    return Action.ATTACH


@dataclass(frozen=True)
class ReattachRecord:
    workflow_id: str
    spec_name: str
    submitted_at: str
    ar_guid: str | None
    generation: int = 0  # not serialised; generation of the object this was read from

    def to_json(self) -> str:
        data = asdict(self)
        data.pop('generation')
        return json.dumps(data)


def now_iso() -> str:
    return datetime.datetime.now(datetime.UTC).isoformat(timespec='seconds')


class ReattachStore:
    def __init__(self, root: str, storage: Storage) -> None:
        self.root = root
        self.storage = storage

    def uri(self, key: str) -> str:
        return join(self.root, f'{key}.json')

    def read(self, key: str) -> ReattachRecord | None:
        blob = self.storage.read(self.uri(key))
        if blob is None:
            return None
        try:
            data = json.loads(blob.data)
            return ReattachRecord(
                workflow_id=data['workflow_id'],
                spec_name=data.get('spec_name', ''),
                submitted_at=data.get('submitted_at', ''),
                ar_guid=data.get('ar_guid'),
                generation=blob.generation,
            )
        except (ValueError, KeyError, TypeError):
            # unreadable record: treat as absent, but replace it (not create) when we write
            return ReattachRecord('', '', '', None, generation=blob.generation)

    def write(self, key: str, record: ReattachRecord, *, replace_generation: int | None) -> None:
        """
        Create (replace_generation=None -> if_generation_match=0) or replace the record.
        Raises PreconditionFailed if another process wrote first.
        """
        self.storage.write(
            self.uri(key),
            record.to_json(),
            if_generation_match=0 if replace_generation is None else replace_generation,
        )


__all__ = [
    'Action',
    'PreconditionFailed',
    'ReattachRecord',
    'ReattachStore',
    'action_for',
    'canonical_json',
    'now_iso',
    'reattach_key',
]
