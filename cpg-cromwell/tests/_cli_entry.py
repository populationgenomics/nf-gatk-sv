"""Run `cpg-cromwell` in a subprocess against the fake server: <url> <gcs_root> <cli args...>."""

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from cpg_cromwell.cli import main
from tests.conftest import DirStorage, FakeCromwell, make_context


class _Url(FakeCromwell):
    def __init__(self, url: str) -> None:  # just carries the URL; no server in this process
        self._url = url

    @property
    def url(self) -> str:
        return self._url


if __name__ == '__main__':
    url, root, *argv = sys.argv[1:]
    sys.exit(main(argv, context=make_context(_Url(url), DirStorage(Path(root)))))
