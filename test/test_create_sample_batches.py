import json
from pathlib import Path

import pandas as pd
import pytest
from tests.conftest import DirStorage

from cpg_cromwell import EXIT_FAILED, EXIT_OK, RunContext, SpecError
from gatk_sv_nf import cli
from gatk_sv_nf.manifest import Cohort, Manifest, SequencingGroup
from gatk_sv_nf.scripts import sample_batching
from gatk_sv_nf.stages import create_sample_batches as csb
from gatk_sv_nf.stages import evidence_qc

N_SGS = 12
SG_IDS = [f'CPG{i:02d}' for i in range(N_SGS)]
# CPG00-05 in ds-one (COH1), CPG06-11 in ds-two (COH2); every third SG is on a PCR-free library
SGS = tuple(
    SequencingGroup(
        id=sg_id,
        dataset='ds-one' if i < 6 else 'ds-two',
        cram=None,
        meta={'library_type': 'pcr-free' if i % 3 == 0 else 'pcr+'} if i != 11 else {'facility': 'x'},
    )
    for i, sg_id in enumerate(SG_IDS)
)
COHORTS = (
    Cohort(id='COH1', name='one', dataset='ds-one', sequencing_group_ids=tuple(SG_IDS[:6])),
    Cohort(id='COH2', name='two', dataset='ds-two', sequencing_group_ids=tuple(SG_IDS[6:])),
)
MANIFEST = Manifest(
    name='mc',
    analysis_dataset='analysis-ds',
    cohorts=COHORTS,
    active_sg_ids={'ds-one': tuple(SG_IDS[:6]), 'ds-two': tuple(SG_IDS[6:])},
    sequencing_groups=SGS,
    output_version='ov_12',
)
OUTPUT = 'gs://cpg-analysis-ds-test/gatk_sv/ov_12/CreateSampleBatches/sgid_batches.json'
SGS_META = 'gs://cpg-analysis-ds-test-tmp/gatk_sv/ov_12/CreateSampleBatches/sgs_meta.json'


def _storage_section(ds: str) -> str:
    return f'\n[storage.{ds}]\ndefault = "gs://cpg-{ds}-test"\ntmp = "gs://cpg-{ds}-test-tmp"\n'


@pytest.fixture
def batch_config(write_config) -> Path:
    from conftest import BASE_CONFIG
    from cpg_utils import config as cpg_config

    path = write_config()  # registers the cache reset
    text = BASE_CONFIG.replace('[workflow]\n', '[workflow]\nmin_batch_size = 10\nmax_batch_size = 300\n')
    path.write_text(text + _storage_section('ds-one') + _storage_section('ds-two'))
    cpg_config.set_config_paths([str(path)])
    return path


def qc_table(sg_ids: list[str], extra: list[str] = ()) -> str:
    """An EvidenceQC QC table: alternate sexes, coverage rising with the SG number. `extra` aren't in the run."""
    rows = ['#sample_id\tchrX_CopyNumber_rounded\tmedian_coverage']
    for sg_id in [*sg_ids, *extra]:
        n = int(sg_id[3:])
        rows.append(f'{sg_id}\t{1 if n % 2 else 2}\t{30 + n}.5')
    return '\n'.join(rows) + '\n'


def put_tables(storage: DirStorage) -> None:
    for cohort in COHORTS:
        extra = ['CPG99'] if cohort.id == 'COH2' else []
        storage.put(
            evidence_qc.expected_outputs(cohort)['qc_table'], qc_table(list(cohort.sequencing_group_ids), extra)
        )


# ---- the copied script (P4) ------------------------------------------------------------------------------------


def _md(n: int) -> pd.DataFrame:
    return pd.DataFrame({
        'sample_id': [f'S{i:02d}' for i in range(n)],
        'chrX_CopyNumber_rounded': [1 if i % 2 else 2 for i in range(n)],
        'median_coverage': [float(30 + (i * 7) % n) for i in range(n)],
    })  # fmt: skip


def test_batch_sgs_one_batch_when_in_range() -> None:
    [batch] = sample_batching.batch_sgs(_md(12), min_batch_size=10, max_batch_size=300)
    assert batch['size'] == 12
    assert batch['sequencing_groups'] == [f'S{i:02d}' for i in range(12)]  # input order
    assert (batch['male_count'], batch['female_count']) == (6, 6)
    assert batch['mf_ratio'] == 1.0


def test_batch_sgs_splits_by_coverage_keeping_the_sex_ratio() -> None:
    batches = sample_batching.batch_sgs(_md(20), min_batch_size=2, max_batch_size=8)
    # 20 // 2 = 10 > 8, so 3 bins; each sex (10 apiece) is split 4/3/3 by np.array_split
    assert [b['size'] for b in batches] == [8, 6, 6]
    assert [(b['male_count'], b['female_count']) for b in batches] == [(4, 4), (3, 3), (3, 3)]
    assert sorted(s for b in batches for s in b['sequencing_groups']) == [f'S{i:02d}' for i in range(20)]
    # lowest coverage first, within each sex
    medians = [b['batch_median_coverage'] for b in batches]
    assert medians == sorted(medians)


def test_integer_coverage_fails_as_in_cpg_flow(tmp_path: Path, batch_config: Path) -> None:
    """
    Inherited, kept on purpose (plan §8): with whole-number coverage the one-batch path puts np.int64 into
    `coverage_range`, and json.dump fails. cpg-flow-gatk-sv's script fails the same way. Fix both or neither.
    """
    table = tmp_path / 't.tsv'
    table.write_text(qc_table(SG_IDS).replace('.5\n', '\n'))
    (tmp_path / 's.json').write_text(json.dumps({s: {} for s in SG_IDS}))
    with pytest.raises(TypeError, match='int64 is not JSON serializable'):
        sample_batching.partition_batches([str(table)], str(tmp_path / 's.json'), str(tmp_path / 'o.json'))


def test_batch_sgs_by_library() -> None:
    md = sample_batching.add_sg_meta_fields(
        _md(4), {'S00': {'library_type': 'a'}, 'S01': {'sequencing_library': 'b'}, 'S02': {}, 'S03': {'facility': 'f'}}
    )
    assert md['library'].tolist() == ['a', 'b', 'unknown', 'unknown']
    assert md['facility'].tolist() == ['unknown', 'unknown', 'unknown', 'f']
    batches = sample_batching.batch_sgs_by_library(md, min_batch_size=1, max_batch_size=10)
    assert [b['sequencing_groups'] for b in batches] == [['S00'], ['S01'], ['S02', 'S03']]


# ---- the stage ---------------------------------------------------------------------------------------------------


def test_paths(batch_config: Path) -> None:
    assert csb.expected_outputs(MANIFEST) == {'sgid_batches': OUTPUT}
    assert csb.sgs_meta_path(MANIFEST) == SGS_META
    assert csb.qc_tables(MANIFEST) == [
        'gs://cpg-ds-one-test/gatk_sv/COH1/EvidenceQC/evidence_qc_table.tsv',
        'gs://cpg-ds-two-test/gatk_sv/COH2/EvidenceQC/evidence_qc_table.tsv',
    ]


def test_run(batch_config: Path, storage: DirStorage, tmp_path: Path) -> None:
    put_tables(storage)
    created: list[tuple] = []

    def registrar(manifest, outputs):
        def create(project, output, atype, sg_ids, cohort_ids, meta):
            created.append((project, output, atype, sg_ids, cohort_ids, meta))
            return 1

        return csb.register_outputs(manifest, outputs, create=create, size_of=lambda _o: 9)

    upstream = [{'qc_table': t} for t in reversed(csb.qc_tables(MANIFEST))]  # channel order doesn't matter
    code = csb.run(
        MANIFEST, out=tmp_path / 'outputs.json', upstream=upstream, registrar=registrar,
        context=RunContext(storage=storage),
    )  # fmt: skip
    assert code == EXIT_OK

    # sgs_meta.json exactly as cpg-flow writes it: json.dump, default separators, manifest SG order
    assert storage.text(SGS_META) == json.dumps({sg.id: sg.meta for sg in SGS})
    # one batch per library (groupby sorts them), each males then females, by coverage; CPG99 isn't in the run
    batches = json.loads(storage.text(OUTPUT))
    assert [b['sequencing_groups'] for b in batches] == [
        ['CPG01', 'CPG05', 'CPG07', 'CPG02', 'CPG04', 'CPG08', 'CPG10'],  # pcr+
        ['CPG03', 'CPG09', 'CPG00', 'CPG06'],  # pcr-free
        ['CPG11'],  # no library in the meta
    ]
    assert json.loads((tmp_path / 'outputs.json').read_text()) == {'sgid_batches': OUTPUT}

    meta = {'stage': 'CreateSampleBatches', 'sequencing_type': 'genome', 'datasets': ['ds-one', 'ds-two'],
            'cohorts': ['COH1', 'COH2'], 'size': 9}  # fmt: skip
    assert created == [('analysis-ds-test', OUTPUT, 'sv', [], ['COH1', 'COH2'], meta)]


def test_run_matches_the_script_on_the_same_files(batch_config: Path, storage: DirStorage, tmp_path: Path) -> None:
    """The stage adds nothing to the script's output: same bytes as running it on the tables directly."""
    put_tables(storage)
    assert csb.run(MANIFEST, out=tmp_path / 'o.json', registrar=None, context=RunContext(storage=storage)) == 0

    tables = []
    for i, uri in enumerate(csb.qc_tables(MANIFEST)):
        tables.append(str(tmp_path / f't{i}.tsv'))
        Path(tables[-1]).write_text(storage.text(uri))
    (tmp_path / 'sgs.json').write_text(storage.text(SGS_META))
    sample_batching.partition_batches(tables, str(tmp_path / 'sgs.json'), str(tmp_path / 'direct.json'))
    assert storage.text(OUTPUT) == (tmp_path / 'direct.json').read_text()


def test_run_reuses_existing_output(batch_config: Path, storage: DirStorage, tmp_path: Path) -> None:
    storage.put(OUTPUT, '[]')
    code = csb.run(
        MANIFEST, out=tmp_path / 'outputs.json', registrar=lambda *_: pytest.fail('registered a reused stage'),
        context=RunContext(storage=storage),
    )  # fmt: skip
    assert code == EXIT_OK
    assert storage.text(OUTPUT) == '[]'
    assert not storage.exists(SGS_META)  # nothing ran


def test_run_refuses_too_few_sgs(batch_config: Path, storage: DirStorage, tmp_path: Path) -> None:
    small = Manifest(**{**MANIFEST.__dict__, 'sequencing_groups': SGS[:9]})
    with pytest.raises(csb.TooFewSequencingGroupsError, match='9 sequencing group'):
        csb.run(small, out=tmp_path / 'o.json', context=RunContext(storage=storage))
    assert not storage.exists(SGS_META)


def test_run_checks_upstream(batch_config: Path, storage: DirStorage, tmp_path: Path) -> None:
    put_tables(storage)
    one = [{'qc_table': csb.qc_tables(MANIFEST)[0]}]
    with pytest.raises(SpecError, match='not one per cohort'):
        csb.run(MANIFEST, out=tmp_path / 'o.json', upstream=one, context=RunContext(storage=storage))


def test_run_registration_failure(batch_config: Path, storage: DirStorage, tmp_path: Path) -> None:
    put_tables(storage)

    def broken(*_a):
        raise ConnectionError('metamist down')

    assert csb.run(MANIFEST, out=tmp_path / 'o.json', registrar=broken, context=RunContext(storage=storage)) == (
        EXIT_FAILED
    )
    assert storage.exists(OUTPUT)
    assert not (tmp_path / 'o.json').exists()


def test_cli(batch_config: Path, storage: DirStorage, tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    put_tables(storage)
    plan = tmp_path / 'plan'
    plan.mkdir()
    (plan / 'config.toml').write_text(batch_config.read_text())
    (plan / 'manifest.json').write_text(MANIFEST.to_json())
    ups = []
    for i, table in enumerate(csb.qc_tables(MANIFEST)):
        (tmp_path / f'up{i}.json').write_text(json.dumps({'qc_table': table}))
        ups += ['--upstream', f'EvidenceQC={tmp_path / f"up{i}.json"}']
    monkeypatch.setattr(cli.RunContext, 'resolved', lambda self: RunContext(storage=storage))

    out = tmp_path / 'outputs.json'
    args = ['stage', 'CreateSampleBatches', '--plan', str(plan), *ups, '--out', str(out), '--no-register']
    assert cli.main(args) == EXIT_OK
    assert json.loads(out.read_text()) == {'sgid_batches': OUTPUT}

    assert cli.main([*args, '--upstream', f'TrainGcnv={tmp_path / "up0.json"}']) == 2
    assert cli.main([*args, '--upstream', 'EvidenceQC']) == 2
