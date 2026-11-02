"""The package must stay Hail-free and independent of gatk_sv_nf (design §3)."""

import importlib.util
import subprocess
import sys

FORBIDDEN = ('hail', 'hailtop', 'gatk_sv_nf', 'cpg_utils.cromwell', 'cpg_utils.hail_batch', 'cpg_utils.dataproc')

CHECK = f"""
import importlib, pkgutil, sys
import cpg_cromwell
for m in pkgutil.walk_packages(cpg_cromwell.__path__, 'cpg_cromwell.'):
    if m.name != 'cpg_cromwell.__main__':
        importlib.import_module(m.name)
# exercise the lazily-imported paths too
import cpg_utils.cloud, cpg_utils.config, cpg_utils.constants, cpg_utils.cromwell_model
bad = sorted(m for m in sys.modules if m in {FORBIDDEN!r} or m.startswith(('hail.', 'hailtop.', 'gatk_sv_nf.')))
print(','.join(bad))
"""


def test_no_forbidden_imports() -> None:
    out = subprocess.run([sys.executable, '-c', CHECK], capture_output=True, text=True, check=True)
    assert out.stdout.strip() == ''


def test_env_has_no_hail_or_gatk_sv_nf() -> None:
    for name in ('hail', 'hailtop', 'gatk_sv_nf'):
        assert importlib.util.find_spec(name) is None, f'{name} is installed in the cpg-cromwell test env'
