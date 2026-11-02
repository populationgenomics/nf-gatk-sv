import pytest

from cpg_cromwell.reattach import Action, ReattachRecord, ReattachStore, action_for
from cpg_cromwell.storage import PreconditionFailed
from tests.conftest import DirStorage


@pytest.mark.parametrize(
    ('status', 'action'),
    [
        (None, Action.SUBMIT),
        ('Submitted', Action.ATTACH),
        ('Running', Action.ATTACH),
        ('On Hold', Action.ATTACH),
        ('Aborting', Action.WAIT_ABORTED),
        ('Succeeded', Action.COLLECT),
        ('Failed', Action.SUBMIT),
        ('Aborted', Action.SUBMIT),
    ],
)
def test_state_machine(status: str | None, action: Action) -> None:
    assert action_for(status) is action


def test_record_create_and_replace_with_preconditions(storage: DirStorage) -> None:
    store = ReattachStore('gs://tmp/ids', storage)
    assert store.read('k') is None
    store.write('k', ReattachRecord('wf-1', 'name', 't', 'g1'), replace_generation=None)
    # a second creator loses the race
    with pytest.raises(PreconditionFailed):
        store.write('k', ReattachRecord('wf-2', 'name', 't', 'g2'), replace_generation=None)
    rec = store.read('k')
    assert rec is not None
    assert (rec.workflow_id, rec.ar_guid) == ('wf-1', 'g1')
    # replace with the generation we read
    store.write('k', ReattachRecord('wf-3', 'name', 't', 'g3'), replace_generation=rec.generation)
    # a stale generation loses
    with pytest.raises(PreconditionFailed):
        store.write('k', ReattachRecord('wf-4', 'name', 't', 'g4'), replace_generation=rec.generation)
    assert store.read('k').workflow_id == 'wf-3'  # type: ignore[union-attr]
