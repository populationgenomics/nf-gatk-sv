import pytest

from cpg_cromwell.collect import collect_outputs, plan_collection
from cpg_cromwell.errors import CollectError
from tests.conftest import DirStorage, make_spec


def test_missing_and_null() -> None:
    spec = make_spec(collect={'out': 'gs://d/o'})
    with pytest.raises(CollectError, match=r'Wfl\.out is missing'):
        plan_collection(spec, {})
    with pytest.raises(CollectError, match='null'):
        plan_collection(spec, {'Wfl.out': None})


def test_list_length_must_match() -> None:
    spec = make_spec(collect={'outs': ['gs://d/a', 'gs://d/b']})
    with pytest.raises(CollectError, match='3 elements, expected 2'):
        plan_collection(spec, {'Wfl.outs': ['gs://e/1', 'gs://e/2', 'gs://e/3']})
    with pytest.raises(CollectError, match='not an array'):
        plan_collection(spec, {'Wfl.outs': 'gs://e/1'})
    with pytest.raises(CollectError, match='null'):
        plan_collection(spec, {'Wfl.outs': ['gs://e/1', None]})


def test_single_destination_rejects_array() -> None:
    spec = make_spec(collect={'out': 'gs://d/o'})
    with pytest.raises(CollectError, match='expected a single value'):
        plan_collection(spec, {'Wfl.out': ['gs://e/1']})


def test_scalars_pass_through_and_paths_are_copied() -> None:
    spec = make_spec(
        collect={'n': 'gs://unused', 'flag': 'gs://unused2', 'out': 'gs://d/o', 'outs': ['gs://d/a', 'gs://d/b']}
    )
    result, copies = plan_collection(
        spec,
        {'Wfl.n': 3, 'Wfl.flag': False, 'Wfl.out': 'gs://e/o', 'Wfl.outs': ['gs://e/a', 'gs://e/b'], 'Wfl.extra': 1},
    )
    assert result == {'n': 3, 'flag': False, 'out': 'gs://d/o', 'outs': ['gs://d/a', 'gs://d/b']}
    assert copies == [('gs://e/o', 'gs://d/o'), ('gs://e/a', 'gs://d/a'), ('gs://e/b', 'gs://d/b')]


def test_copies_and_skips_same_path(storage: DirStorage) -> None:
    storage.put('gs://e/a', 'A')
    storage.put('gs://same/x', 'X')
    spec = make_spec(collect={'a': 'gs://d/a', 'x': 'gs://same/x'})
    result = collect_outputs(spec, {'Wfl.a': 'gs://e/a', 'Wfl.x': 'gs://same/x'}, storage)
    assert result == {'a': 'gs://d/a', 'x': 'gs://same/x'}
    assert storage.text('gs://d/a') == 'A'


def test_missing_source_is_collect_error(storage: DirStorage) -> None:
    spec = make_spec(collect={'a': 'gs://d/a'})
    with pytest.raises(CollectError, match='not found'):
        collect_outputs(spec, {'Wfl.a': 'gs://e/nope'}, storage)


def test_no_collect_returns_raw_outputs(storage: DirStorage) -> None:
    spec = make_spec(collect=None)
    assert collect_outputs(spec, {'Wfl.a': 1}, storage) == {'Wfl.a': 1}
