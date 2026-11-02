import functools
import json
from pathlib import Path

import pytest
from tests.conftest import DirStorage, FakeCromwell, make_context, write_bundle

from cpg_cromwell import EXIT_FAILED, EXIT_OK, SpecError, render
from gatk_sv_nf.manifest import SequencingGroup
from gatk_sv_nf.stages import gather_sample_evidence as gse
from gatk_sv_nf.stages._cromwell_base import default_bundle

SG = SequencingGroup(
    id='CPG111', dataset='fixture-ds', cram='gs://cpg-fixture-ds-test/cram/CPG111.cram', participant_id='P1'
)
EVIDENCE = 'gs://cpg-fixture-ds-test/sv_evidence'
GATK_SV_COMMIT = '890582922bccb4b651275506a34311c949417f6c'  # as in conftest.BASE_CONFIG
OFFLINE_SECRET = json.dumps({'client_email': 'sa@example.com'})


def never(_uri: str) -> bool:
    return False


def test_expected_outputs_default(config: Path) -> None:
    out = gse.expected_outputs(SG)
    assert out['coverage_counts'] == f'{EVIDENCE}/CPG111.coverage_counts.tsv.gz'
    assert out['pesr_split'] == f'{EVIDENCE}/CPG111.sr.txt.gz'
    assert out['pesr_sd'] == f'{EVIDENCE}/CPG111.sd.txt.gz'
    assert out['pesr_disc'] == f'{EVIDENCE}/CPG111.pe.txt.gz'
    assert out['scramble_index'] == f'{EVIDENCE}/CPG111.scramble.vcf.gz.tbi'
    assert list(out) == [
        'coverage_counts',
        'pesr_split',
        'pesr_split_index',
        'pesr_sd',
        'pesr_sd_index',
        'pesr_disc',
        'pesr_disc_index',
        'manta_vcf',
        'manta_index',
        'wham_vcf',
        'wham_index',
        'scramble_vcf',
        'scramble_index',
    ]


def test_expected_outputs_scramble_only_also_expects_manta(write_config) -> None:
    write_config('\n[workflow.GatherSampleEvidence]\nonly_jobs = ["scramble"]\n')
    assert sorted(gse.expected_outputs(SG)) == ['manta_index', 'manta_vcf', 'scramble_index', 'scramble_vcf']
    assert gse.analysis_keys() == ['scramble_vcf']


def test_inputs_default(config: Path) -> None:
    expected = gse.expected_outputs(SG)
    inputs, collect = gse.inputs(SG, expected, never)
    assert collect == expected
    assert inputs['bam_or_cram_index'] == 'gs://cpg-fixture-ds-test/cram/CPG111.cram.crai'
    assert inputs['reference_dict'] == 'gs://cpg-common-main/references/hg38/Homo_sapiens_assembly38_masked.dict'
    assert inputs['reference_index'].endswith('masked.fasta.fai')
    assert inputs['sd_locs_vcf'] == 'gs://ref/dbsnp.vcf'
    assert inputs['move_bam_or_cram_files'] is True
    assert 'collect_coverage' not in inputs
    assert 'collect_pesr' not in inputs
    assert 'manta_vcf_input' not in inputs


def test_inputs_scramble_only_reuses_existing_manta(write_config) -> None:
    write_config('\n[workflow.GatherSampleEvidence]\nonly_jobs = ["scramble"]\n')
    expected = gse.expected_outputs(SG)
    inputs, collect = gse.inputs(SG, expected, lambda uri: uri.endswith('.manta.vcf.gz'))
    assert inputs['manta_vcf_input'] == f'{EVIDENCE}/CPG111.manta.vcf.gz'
    assert inputs['manta_vcf_index_input'] == f'{EVIDENCE}/CPG111.manta.vcf.gz.tbi'
    assert sorted(collect) == ['scramble_index', 'scramble_vcf']
    assert 'manta_vcf' in expected  # the skip check still wants it
    assert (inputs['collect_coverage'], inputs['collect_pesr']) == (False, False)
    assert (inputs['manta_docker'], inputs['wham_docker'], inputs['scramble_docker']) == (None, None, 'img/scramble')


def test_inputs_scramble_only_runs_manta_when_missing(write_config) -> None:
    write_config('\n[workflow.GatherSampleEvidence]\nonly_jobs = ["scramble"]\n')
    expected = gse.expected_outputs(SG)
    inputs, collect = gse.inputs(SG, expected, never)
    assert 'manta_vcf_input' not in inputs
    assert sorted(collect) == ['manta_index', 'manta_vcf', 'scramble_index', 'scramble_vcf']
    assert (inputs['manta_docker'], inputs['wham_docker']) == ('img/manta', None)
    assert gse.only_jobs() == ['scramble']  # the config itself wasn't mutated


def test_spec(config: Path) -> None:
    spec = gse.build_sg_spec(SG, ar_guid='guid-1', exists=never)
    assert spec.name == 'GatherSampleEvidence/fixture-ds/CPG111'
    assert spec.output_prefix == 'gatk_sv/output/GatherSampleEvidence/fixture-ds/CPG111'
    assert spec.dataset == 'analysis-ds'  # workflow.dataset, not the SG's
    assert spec.access_level == 'test'
    assert spec.source.bundle == default_bundle()
    assert spec.source.bundle == f'gs://cpg-analysis-ds-test-tmp/gatk_sv_nf/bundles/gatk-sv/{GATK_SV_COMMIT}/'
    assert spec.source.workflow == 'GatherSampleEvidence.wdl'
    assert spec.labels == {'dataset': 'fixture-ds', 'sequencing-group': 'cpg111', 'stage': 'gathersampleevidence'}
    assert spec.inputs['runtime_attr_wham'] == {'mem_gb': 32}  # resource override merged
    assert spec.collect == gse.expected_outputs(SG)
    assert (spec.poll.min_seconds, spec.poll.max_seconds, spec.poll.jitter) == (0.01, 0.05, True)
    assert spec.poll.time_limit_seconds == 86400

    inputs, options = render(spec, secret_json=OFFLINE_SECRET)
    assert inputs['GatherSampleEvidence.sample_id'] == 'CPG111'
    assert options['google_labels'] == {
        'dataset': 'fixture-ds',
        'sequencing-group': 'cpg111',
        'stage': 'gathersampleevidence',
        'compute-category': 'cromwell',
        'ar-guid': 'guid-1',
    }
    assert options['google_project'] == 'analysis-ds-project'


def test_no_callers_refuses_to_run(write_config, tmp_path: Path) -> None:
    write_config('\n[workflow.GatherSampleEvidence]\nonly_jobs = ["coverage_counts", "pesr"]\n')
    with pytest.raises(SpecError, match='no SV caller'):
        gse.run_chunk([SG], ar_guid='g', out_dir=tmp_path / 'out', reattach_root=None)


# ---- end to end: fake Cromwell, directory-backed GCS -----------------------------------------------


def test_run_chunk(config: Path, cromwell: FakeCromwell, storage: DirStorage, tmp_path: Path) -> None:
    bundle = default_bundle()
    write_bundle(storage, bundle=bundle, workflow='GatherSampleEvidence.wdl')
    keys = list(gse.expected_outputs(SG))
    cromwell.default_outputs = {f'GatherSampleEvidence.{k}': f'gs://exec/{k}' for k in keys}
    for k in keys:
        storage.put(f'gs://exec/{k}', f'content of {k}')

    reused = SequencingGroup(id='CPG222', dataset='fixture-ds', cram='gs://x/CPG222.cram')
    for path in gse.expected_outputs(reused).values():
        storage.put(path, 'already here')
    failing = SequencingGroup(id='CPG333', dataset='fixture-ds', cram='gs://x/CPG333.cram')
    # submissions happen in spec order: CPG111 then CPG333
    cromwell.scripts = [['Submitted', 'Running', 'Succeeded'], ['Submitted', 'Failed']]

    created: list[tuple] = []

    def create(project, output, atype, sg_ids, cohort_ids, meta):
        created.append((project, output, atype, sg_ids, cohort_ids, meta))
        return len(created)

    registrar = functools.partial(gse.register_outputs, create=create, size_of=lambda _o: 42)
    out_dir = tmp_path / 'outputs'
    report = gse.run_chunk(
        [SG, reused, failing],
        ar_guid='guid-1',
        out_dir=out_dir,
        reattach_root=None,
        registrar=registrar,
        submit_interval=0,
        context=make_context(cromwell, storage),
    )

    assert report.reused == ['CPG222']
    assert [(r.name.rsplit('/', 1)[1], r.status) for r in report.results] == [
        ('CPG111', 'Succeeded'),
        ('CPG333', 'Failed'),
    ]
    assert report.exit_code == EXIT_FAILED
    assert len(cromwell.submissions) == 2  # nothing submitted for the reused SG

    # outputs copied to the cpg-flow paths
    assert storage.text(f'{EVIDENCE}/CPG111.wham.vcf.gz') == 'content of wham_vcf'
    assert json.loads((out_dir / 'CPG111.json').read_text()) == gse.expected_outputs(SG)
    assert json.loads((out_dir / 'CPG222.json').read_text()) == gse.expected_outputs(reused)
    assert not (out_dir / 'CPG333.json').exists()

    # one sv analysis per caller VCF, for the submitted+succeeded SG only
    meta = {
        'stage': 'GatherSampleEvidence',
        'sequencing_type': 'genome',
        'dataset': 'fixture-ds',
        'sequencing_group': 'CPG111',
        'participant_id': 'P1',
        'size': 42,
    }
    assert created == [
        ('fixture-ds-test', f'{EVIDENCE}/CPG111.{c}.vcf.gz', 'sv', ['CPG111'], [], meta)
        for c in ('manta', 'wham', 'scramble')
    ]


def test_run_chunk_registration_failure_fails_the_sg(
    config: Path, cromwell: FakeCromwell, storage: DirStorage, tmp_path: Path
) -> None:
    write_bundle(storage, bundle=default_bundle(), workflow='GatherSampleEvidence.wdl')
    keys = list(gse.expected_outputs(SG))
    cromwell.default_outputs = {f'GatherSampleEvidence.{k}': f'gs://exec/{k}' for k in keys}
    for k in keys:
        storage.put(f'gs://exec/{k}', 'x')

    def broken(_sg, _outputs):
        raise ConnectionError('metamist down')

    report = gse.run_chunk(
        [SG],
        ar_guid='guid-1',
        out_dir=tmp_path / 'outputs',
        reattach_root=None,
        registrar=broken,
        submit_interval=0,
        context=make_context(cromwell, storage),
    )
    [result] = report.results
    assert result.status == 'Succeeded'
    assert result.exit_code != EXIT_OK
    assert 'metamist down' in (result.error or '')
    assert not (tmp_path / 'outputs' / 'CPG111.json').exists()
