import json
from pathlib import Path

import pytest
from tests.conftest import DirStorage, FakeCromwell, make_context, write_bundle

from cpg_cromwell import EXIT_FAILED, EXIT_OK, render
from gatk_sv_nf import cli
from gatk_sv_nf.manifest import Cohort, Manifest, SequencingGroup
from gatk_sv_nf.stages import evidence_qc
from gatk_sv_nf.stages._cromwell_base import default_bundle, run_target

SGS = (
    SequencingGroup(id='CPG20', dataset='fixture-ds', cram=None),
    SequencingGroup(id='CPG10', dataset='fixture-ds', cram=None),
    SequencingGroup(id='CPG30', dataset='analysis-ds', cram=None),
)
COHORT = Cohort(id='COH1', name='cohort one', dataset='analysis-ds', sequencing_group_ids=('CPG20', 'CPG10', 'CPG30'))
OTHER = Cohort(id='COH2', name='cohort two', dataset='fixture-ds', sequencing_group_ids=('CPG10',))
MANIFEST = Manifest(
    name='mc',
    analysis_dataset='analysis-ds',
    cohorts=(COHORT, OTHER),
    active_sg_ids={'fixture-ds': ('CPG20', 'CPG10'), 'analysis-ds': ('CPG30',)},
    sequencing_groups=SGS,
    output_version='ov_3',
)
PREFIX = 'gs://cpg-analysis-ds-test/gatk_sv/COH1/EvidenceQC'


def _config_text() -> str:
    """BASE_CONFIG plus EvidenceQC's keys. TOML can't reopen a table, so they're spliced into the existing ones."""
    from conftest import BASE_CONFIG

    text = BASE_CONFIG.replace('[images]\n', '[images]\nsv_base_docker = "img/sv_base"\n'
                               'sv_pipeline_qc_docker = "img/sv_pipeline_qc"\n')  # fmt: skip
    text = text.replace('[references]\n', '[references]\nwgd_scoring_mask = "gs://ref/wgd_mask.bed"\n')
    return text + '\n[resource_overrides.EvidenceQC.runtime_attr_mediancov]\nmem_gb = 64\n'


def _activate(path: Path, text: str) -> Path:
    from cpg_utils import config as cpg_config

    path.write_text(text)
    cpg_config.set_config_paths([str(path)])
    return path


@pytest.fixture
def qc_config(write_config) -> Path:
    return _activate(write_config(), _config_text())  # write_config registers the cache reset


@pytest.fixture
def fast_qc_config(write_config) -> Path:
    """Polls in fractions of a second: EvidenceQC is MEDIUM (40s/400s by default)."""
    return _activate(write_config(), _config_text() + '\n[cromwell_polling_intervals.medium]\nmin = 0.01\nmax = 0.05\n')


def test_expected_outputs(qc_config: Path) -> None:
    out = evidence_qc.expected_outputs(COHORT)
    assert out['qc_table'] == f'{PREFIX}/evidence_qc_table.tsv'
    assert out['bincov_matrix'] == f'{PREFIX}/EvidenceQC.RD.txt.gz'
    assert out['wham_qc_high'] == f'{PREFIX}/wham_QC.outlier.high'
    assert len(out) == 9 + 6
    # the cohort's dataset, not the SGs'
    assert evidence_qc.expected_outputs(OTHER)['qc_table'] == (
        'gs://cpg-fixture-ds-test/gatk_sv/COH2/EvidenceQC/evidence_qc_table.tsv'
    )


def test_inputs_in_cohort_order(qc_config: Path) -> None:
    inputs = evidence_qc.inputs(MANIFEST, COHORT)
    assert inputs['batch'] == 'COH1'
    assert inputs['samples'] == ['CPG20', 'CPG10', 'CPG30']
    assert inputs['run_vcf_qc'] is True
    assert inputs['counts'] == [
        'gs://cpg-fixture-ds-test/sv_evidence/CPG20.coverage_counts.tsv.gz',
        'gs://cpg-fixture-ds-test/sv_evidence/CPG10.coverage_counts.tsv.gz',
        'gs://cpg-analysis-ds-test/sv_evidence/CPG30.coverage_counts.tsv.gz',
    ]
    assert inputs['scramble_vcfs'][2] == 'gs://cpg-analysis-ds-test/sv_evidence/CPG30.scramble.vcf.gz'
    assert [k for k in inputs if k.endswith('_vcfs')] == ['manta_vcfs', 'wham_vcfs', 'scramble_vcfs']
    assert inputs['genome_file'] == 'gs://ref/wgd_mask.bed'
    assert inputs['sv_pipeline_qc_docker'] == 'img/sv_pipeline_qc'


def test_inputs_ignore_only_jobs(qc_config: Path) -> None:
    _activate(qc_config, _config_text() + '\n[workflow.GatherSampleEvidence]\nonly_jobs = ["manta"]\n')
    assert len(evidence_qc.inputs(MANIFEST, COHORT)['wham_vcfs']) == 3


def test_spec(qc_config: Path) -> None:
    spec = evidence_qc.build_cohort_spec(MANIFEST, COHORT, ar_guid='guid-1')
    assert spec.name == 'EvidenceQC/analysis-ds'
    assert spec.output_prefix == 'gatk_sv/output/EvidenceQC/analysis-ds'
    assert spec.source.bundle == default_bundle()
    assert spec.source.workflow == 'EvidenceQC.wdl'
    assert spec.labels == {'stage': 'evidenceqc'}
    assert spec.inputs['runtime_attr_mediancov'] == {'mem_gb': 64}
    assert spec.collect == evidence_qc.expected_outputs(COHORT)
    assert (spec.poll.min_seconds, spec.poll.max_seconds) == (40, 400)  # MEDIUM, no override

    inputs, options = render(spec, secret_json=json.dumps({'client_email': 'sa@example.com'}))
    assert inputs['EvidenceQC.samples'] == ['CPG20', 'CPG10', 'CPG30']
    assert options['google_labels'] == {'stage': 'evidenceqc', 'compute-category': 'cromwell', 'ar-guid': 'guid-1'}


def _plan_dir(tmp_path: Path) -> Path:
    plan = tmp_path / 'plan'
    plan.mkdir()
    (plan / 'config.toml').write_text(_config_text())
    (plan / 'manifest.json').write_text(MANIFEST.to_json())
    return plan


@pytest.mark.usefixtures('qc_config')
def test_cli_dump_inputs(tmp_path: Path) -> None:
    plan = _plan_dir(tmp_path)
    dump = tmp_path / 'dump.json'
    code = cli.main(['stage', 'EvidenceQC', '--plan', str(plan), '--target', 'COH1', '--ar-guid', 'g',
                     '--bundle', 'gs://b/gatk-sv/c/', '--dump-inputs', str(dump)])  # fmt: skip
    assert code == EXIT_OK
    entry = json.loads(dump.read_text())['COH1']
    assert entry['inputs']['EvidenceQC.batch'] == 'COH1'
    assert entry['output_prefix'] == 'gatk_sv/output/EvidenceQC/analysis-ds'
    assert entry['poll'] == {'min': 40, 'max': 400}


def test_cli_unknown_cohort(tmp_path: Path, qc_config: Path) -> None:
    plan = _plan_dir(tmp_path)
    code = cli.main(['stage', 'EvidenceQC', '--plan', str(plan), '--target', 'NOPE', '--ar-guid', 'g',
                     '--dump-inputs', str(tmp_path / 'x.json')])  # fmt: skip
    assert code == 2


# ---- end to end: fake Cromwell, directory-backed GCS -----------------------------------------------


def _run(storage: DirStorage, cromwell: FakeCromwell, tmp_path: Path, registrar) -> int:
    return run_target(
        'EvidenceQC/COH1',
        evidence_qc.expected_outputs(COHORT),
        lambda: evidence_qc.build_cohort_spec(MANIFEST, COHORT, ar_guid='guid-1'),
        out=tmp_path / 'outputs.json',
        reattach_root=None,
        registrar=registrar,
        result_file=tmp_path / 'result.json',
        context=make_context(cromwell, storage),
    )


def _wdl_outputs(storage: DirStorage, cromwell: FakeCromwell) -> None:
    write_bundle(storage, bundle=default_bundle(), workflow='EvidenceQC.wdl')
    keys = list(evidence_qc.expected_outputs(COHORT))
    cromwell.default_outputs = {f'EvidenceQC.{k}': f'gs://exec/{k}' for k in keys}
    for k in keys:
        storage.put(f'gs://exec/{k}', f'content of {k}')


def test_run_registers_and_writes_outputs(
    fast_qc_config: Path, cromwell: FakeCromwell, storage: DirStorage, tmp_path: Path
) -> None:
    _wdl_outputs(storage, cromwell)
    created: list[tuple] = []

    def create(project, output, atype, sg_ids, cohort_ids, meta):
        created.append((project, output, atype, sg_ids, cohort_ids, meta))
        return 7

    code = _run(
        storage,
        cromwell,
        tmp_path,
        lambda outputs: evidence_qc.register_outputs(COHORT, outputs, create=create, size_of=lambda _o: 5),
    )
    assert code == EXIT_OK
    assert storage.text(f'{PREFIX}/evidence_qc_table.tsv') == 'content of qc_table'
    assert json.loads((tmp_path / 'outputs.json').read_text()) == evidence_qc.expected_outputs(COHORT)
    meta = {'stage': 'EvidenceQC', 'sequencing_type': 'genome', 'size': 5}
    assert created == [('analysis-ds-test', f'{PREFIX}/evidence_qc_table.tsv', 'qc', [], ['COH1'], meta)]
    assert json.loads((tmp_path / 'result.json').read_text())['status'] == 'Succeeded'

    # a second run reuses the outputs: nothing submitted, nothing registered
    (tmp_path / 'outputs.json').unlink()
    assert _run(storage, cromwell, tmp_path, lambda _o: pytest.fail('registered a reused stage')) == EXIT_OK
    assert len(cromwell.submissions) == 1
    assert (tmp_path / 'outputs.json').exists()


def test_run_failed_workflow(fast_qc_config: Path, cromwell: FakeCromwell, storage: DirStorage, tmp_path: Path) -> None:
    _wdl_outputs(storage, cromwell)
    cromwell.scripts = [['Submitted', 'Failed']]
    assert _run(storage, cromwell, tmp_path, None) == EXIT_FAILED
    assert not (tmp_path / 'outputs.json').exists()


def test_run_registration_failure(
    fast_qc_config: Path, cromwell: FakeCromwell, storage: DirStorage, tmp_path: Path
) -> None:
    _wdl_outputs(storage, cromwell)

    def broken(_outputs):
        raise ConnectionError('metamist down')

    assert _run(storage, cromwell, tmp_path, broken) == EXIT_FAILED
    assert not (tmp_path / 'outputs.json').exists()
    assert 'metamist down' in json.loads((tmp_path / 'result.json').read_text())['error']
