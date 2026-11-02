"""Bundling from a real (local) git repo: main check, upload layout, caching, reproducible zip."""

import json
import subprocess
import zipfile
from io import BytesIO
from pathlib import Path

import pytest

from cpg_cromwell.bundle import build_bundle, load_bundle, make_deps_zip
from cpg_cromwell.errors import BundleError, SpecError
from cpg_cromwell.spec import WorkflowSource
from tests.conftest import DirStorage

GIT = ['git', '-c', 'user.name=t', '-c', 'user.email=t@t', '-c', 'commit.gpgsign=false']


def git(repo: Path, *args: str) -> str:
    return subprocess.run([*GIT, *args], cwd=repo, check=True, capture_output=True, text=True).stdout.strip()


@pytest.fixture
def origin(tmp_path: Path) -> tuple[Path, str, str]:
    """A repo with one commit on main and one on a feature branch."""
    repo = tmp_path / 'origin'
    (repo / 'wdl' / 'sub').mkdir(parents=True)
    git(repo, 'init', '-q', '-b', 'main')
    (repo / 'wdl' / 'Wfl.wdl').write_text('version 1.0\nimport "Structs.wdl"\nworkflow Wfl {}\n')
    (repo / 'wdl' / 'Structs.wdl').write_text('version 1.0\n')
    (repo / 'wdl' / 'sub' / 'Task.wdl').write_text('version 1.0\n')
    (repo / 'wdl' / 'script.py').write_text('print(1)\n')
    (repo / 'README').write_text('x\n')
    git(repo, 'add', '.')
    git(repo, 'commit', '-q', '-m', 'main commit')
    on_main = git(repo, 'rev-parse', 'HEAD')
    git(repo, 'checkout', '-q', '-b', 'feature')
    (repo / 'wdl' / 'Wfl.wdl').write_text('version 1.0\nworkflow Wfl { meta {} }\n')
    git(repo, 'commit', '-q', '-am', 'feature commit')
    off_main = git(repo, 'rev-parse', 'HEAD')
    git(repo, 'checkout', '-q', 'main')
    return repo, on_main, off_main


def _build(storage: DirStorage, repo: Path, commit: str, *, require_main: bool, dest: str = 'gs://b/bundles') -> str:
    return build_bundle(
        'gatk-sv', commit, cwd='wdl', dest=dest, require_main=require_main, storage=storage, repo_url=str(repo)
    )


def test_bundle_on_main(origin: tuple[Path, str, str], storage: DirStorage) -> None:
    repo, on_main, _ = origin
    uri = _build(storage, repo, on_main, require_main=True)
    assert uri == f'gs://b/bundles/gatk-sv/{on_main}/'
    manifest = json.loads(storage.text(uri + 'bundle.json'))  # type: ignore[arg-type]
    assert manifest['verified_on_main'] is True
    assert (manifest['commit'], manifest['cwd'], manifest['libs']) == (on_main, 'wdl', ['.'])
    assert storage.text(uri + 'sub/Task.wdl') == 'version 1.0\n'
    assert storage.text(uri + 'script.py') is None  # only .wdl files are uploaded loose
    names = zipfile.ZipFile(BytesIO(storage.read(uri + 'deps.zip').data)).namelist()  # type: ignore[union-attr]
    assert names == ['Structs.wdl', 'Wfl.wdl', 'script.py', 'sub/Task.wdl']
    loaded = load_bundle(WorkflowSource(uri, 'Wfl.wdl'), 'standard', storage)
    assert loaded.workflow_source.startswith(b'version 1.0\nimport')


def test_commit_off_main_is_refused_unless_test(origin: tuple[Path, str, str], storage: DirStorage) -> None:
    repo, _, off_main = origin
    with pytest.raises(BundleError, match='not merged into the main branch'):
        _build(storage, repo, off_main, require_main=True)
    assert storage.text(f'gs://b/bundles/gatk-sv/{off_main}/bundle.json') is None
    uri = _build(storage, repo, off_main, require_main=False)
    assert json.loads(storage.text(uri + 'bundle.json'))['verified_on_main'] is False  # type: ignore[arg-type]
    # and submission refuses it for non-test access levels
    with pytest.raises(BundleError, match='not verified on main'):
        load_bundle(WorkflowSource(uri, 'Wfl.wdl'), 'standard', storage)
    load_bundle(WorkflowSource(uri, 'Wfl.wdl'), 'test', storage)


def test_cached_bundle_is_reused(
    origin: tuple[Path, str, str], storage: DirStorage, monkeypatch: pytest.MonkeyPatch
) -> None:
    repo, on_main, _ = origin
    _build(storage, repo, on_main, require_main=True)
    import cpg_cromwell.bundle as bundle_module

    def no_git(*_a, **_k):  # type: ignore[no-untyped-def]
        raise AssertionError('should not clone again')

    monkeypatch.setattr(bundle_module, '_git_ok', no_git)
    _build(storage, repo, on_main, require_main=True)


def test_unverified_cache_is_reverified(origin: tuple[Path, str, str], storage: DirStorage) -> None:
    repo, on_main, _ = origin
    uri = _build(storage, repo, on_main, require_main=False)
    manifest = json.loads(storage.text(uri + 'bundle.json'))  # type: ignore[arg-type]
    manifest['verified_on_main'] = False
    storage.put(uri + 'bundle.json', json.dumps(manifest))
    _build(storage, repo, on_main, require_main=True)
    assert json.loads(storage.text(uri + 'bundle.json'))['verified_on_main'] is True  # type: ignore[arg-type]


def test_deps_zip_is_reproducible(origin: tuple[Path, str, str], storage: DirStorage) -> None:
    repo, on_main, _ = origin
    a = _build(storage, repo, on_main, require_main=True, dest='gs://b/one')
    (repo / 'wdl' / 'Structs.wdl').touch()  # mtimes differ between clones anyway
    b = _build(storage, repo, on_main, require_main=True, dest='gs://b/two')
    assert storage.read(a + 'deps.zip').data == storage.read(b + 'deps.zip').data  # type: ignore[union-attr]


def test_make_deps_zip_with_named_libs(tmp_path: Path) -> None:
    (tmp_path / 'lib' / 'x').mkdir(parents=True)
    (tmp_path / 'lib' / 'x' / 'a.wdl').write_text('a')
    (tmp_path / 'other.wdl').write_text('o')
    make_deps_zip(tmp_path, ['lib'], tmp_path / 'd.zip')
    assert zipfile.ZipFile(tmp_path / 'd.zip').namelist() == ['lib/x/a.wdl']
    with pytest.raises(SpecError):
        make_deps_zip(tmp_path / 'lib', ['..'], tmp_path / 'e.zip')


@pytest.mark.parametrize('commit', ['main', 'abc123', 'A' * 40])
def test_commit_must_be_full_sha(commit: str, storage: DirStorage) -> None:
    with pytest.raises(SpecError, match='40-character'):
        build_bundle('gatk-sv', commit, dest='gs://b', require_main=False, storage=storage)


def test_missing_bundle_is_bundle_error(storage: DirStorage) -> None:
    with pytest.raises(BundleError, match='cpg-cromwell bundle'):
        load_bundle(WorkflowSource('gs://b/nope/', 'Wfl.wdl'), 'test', storage)
