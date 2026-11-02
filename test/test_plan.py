"""`nf-gatk-sv plan`: config resolution, the manifest, entry checks and the plan dir (plan §5.4, §5.5)."""

import argparse
import json
import logging
from collections.abc import Iterator
from pathlib import Path
from typing import Any

import pytest
import toml
from conftest import DirStorage
from fake_metamist import FakeMetamist, fixture

from cpg_cromwell import EXIT_OK, EXIT_USAGE, RunContext
from gatk_sv_nf import cli, paths
from gatk_sv_nf import config as cfg
from gatk_sv_nf import plan as planning
from gatk_sv_nf.manifest import (
    Manifest,
    ManifestRequest,
    build_manifest,
    clean_ped_family_id,
    combined_ped,
    sg_hash,
)
from gatk_sv_nf.metamist_client import Metamist, MetamistError

REF_PED = 'HG00096\tHG00096\t0\t0\t1\t0\n'
MC_ORDER = ['CPG20', 'CPG40', 'CPG10', 'CPG11', 'CPG12', 'CPG30']


@pytest.fixture(autouse=True)
def reset_cpg_config() -> Iterator[None]:
    yield
    from cpg_utils import config as cpg_config_module

    cpg_config_module._config_paths = []
    cpg_config_module._config = None


def run_config(**workflow: Any) -> dict[str, Any]:
    return {'workflow': {'access_level': 'test', 'input_cohorts': ['COH2', 'COH1'], **workflow}}


def metamist(ns: str = 'test', **overrides: Any) -> tuple[Metamist, FakeMetamist]:
    data = fixture(ns)
    for key, value in overrides.items():
        data[key] = value
    fake = FakeMetamist(data)
    return Metamist(fake.query), fake


def manifest_for(ns: str = 'test', **workflow: Any) -> Manifest:
    config = cfg.merge_layers(
        run_config(access_level=ns if ns == 'test' else 'standard', **workflow),
        dataset='analysis-ds',
        billing_project='bp',
        driver_image='img',
    )
    return build_manifest(ManifestRequest.from_config(config, find_previous_spicy_vcf=True), metamist(ns)[0])


# ---- config ---------------------------------------------------------------------------------------------------


def test_storage_matches_analysis_runner() -> None:
    # copied from a real analysis-runner config (access level full)
    assert cfg.storage_section('acute-care', 'full') == {
        'analysis': 'gs://cpg-acute-care-main-analysis',
        'default': 'gs://cpg-acute-care-main',
        'tmp': 'gs://cpg-acute-care-main-tmp',
        'upload': 'gs://cpg-acute-care-main-upload',
        'web': 'gs://cpg-acute-care-main-web',
        'web_url': 'https://main-web.populationgenomics.org.au/acute-care',
    }
    assert cfg.storage_section('ds', 'test')['tmp'] == 'gs://cpg-ds-test-tmp'
    assert cfg.storage_section('ds', 'test')['web_url'] == 'https://test-web.populationgenomics.org.au/ds'


def test_layers_and_synthesised_keys(caplog: pytest.LogCaptureFixture) -> None:
    run = run_config(
        min_batch_size=50,
        write_vcf=['x'],
        driver_image='mine',
        GatherSampleEvidence={'only_jobs': ['manta']},
    ) | {'storage': {'default': {'default': 'gs://elsewhere'}}, 'images': {'bcftools': 'my/bcftools'}}
    with caplog.at_level(logging.WARNING):
        config = cfg.merge_layers(run, dataset='analysis-ds', billing_project='bp', driver_image='img')
    wf = config['workflow']
    assert wf['min_batch_size'] == 50  # run config over defaults
    assert wf['model_tar_count'] == 290  # defaults kept
    assert config['images']['bcftools'] == 'my/bcftools'
    assert config['images']['strvctvre']  # sibling defaults kept
    assert (wf['dataset'], wf['dataset_gcp_project'], wf['driver_image']) == ('analysis-ds', 'bp', 'img')
    assert 'storage' not in config
    assert 'gencode_gtf_file' not in wf
    for text in ('workflow.write_vcf', 'workflow.driver_image', '[storage.*]'):
        assert text in caplog.text

    with_storage = cfg.with_storage(config, ['ds-one', 'ds-one'])
    assert sorted(with_storage['storage']) == ['analysis-ds', 'default', 'ds-one']
    assert with_storage['storage']['default'] == cfg.storage_section('analysis-ds', 'test')


def test_run_config_dataset_must_match() -> None:
    with pytest.raises(cfg.ConfigError, match='--dataset'):
        cfg.merge_layers(run_config(dataset='other'), dataset='analysis-ds', billing_project='bp', driver_image='i')


@pytest.mark.parametrize(
    ('entry', 'workflow', 'message'),
    [
        ('sample_evidence', {'access_level': 'main'}, 'access_level'),
        ('evidence_qc', {'permit_inactive_cohorts': True}, 'should not tolerate inactive'),
        ('sample_evidence', {'permit_inactive_cohorts': True}, 'should not tolerate inactive'),
        ('multi_sample', {'input_cohorts': None, 'input_datasets': ['ds']}, 'only accepted by the sample_evidence'),
        ('multi_sample', {'input_cohorts': []}, 'non-empty'),
        ('multi_sample', {'populate_assays': True}, 'populate_assays'),
        ('sample_evidence', {'GatherSampleEvidence': {'only_jobs': ['mantaa']}}, 'only_jobs'),
        ('sample_evidence', {'only_sgs': ['a'], 'skip_sgs': ['b']}, 'only_sgs and skip_sgs'),
    ],
)
def test_validate_refuses(entry: str, workflow: dict[str, Any], message: str) -> None:
    config = cfg.merge_layers(run_config(**workflow), dataset='analysis-ds', billing_project='bp', driver_image='i')
    with pytest.raises(cfg.ConfigError, match=message):
        cfg.validate(config, entry)


def test_multi_sample_tolerates_permit_inactive_cohorts() -> None:
    config = cfg.merge_layers(
        run_config(permit_inactive_cohorts=True), dataset='analysis-ds', billing_project='bp', driver_image='i'
    )
    cfg.validate(config, 'multi_sample')


# ---- manifest -------------------------------------------------------------------------------------------------


def test_manifest_follows_cpg_flow_ordering_and_values() -> None:
    m = manifest_for()
    assert m.cohort_ids == ['COH1', 'COH2']  # sorted, not config order
    assert m.cohorts[0].sequencing_group_ids == ('CPG20', 'CPG10', 'CPG11', 'CPG12')
    assert m.cohorts[0].dataset == 'analysis-ds'  # no cohort dataset: the analysis dataset
    assert m.cohorts[0].name == 'first'
    assert m.cohorts[1].dataset == 'ds-one'
    assert m.dataset_names == ['ds-two', 'ds-one']  # first appearance, -test stripped
    assert [sg.id for sg in m.sequencing_groups] == MC_ORDER
    assert m.output_version == sg_hash(MC_ORDER)
    assert m.name.endswith('_2_cohorts')

    sgs = m.sg_by_id()
    assert sgs['CPG10'].cram == 'gs://cpg-ds-one-test/cram/CPG10.cram'  # the later analysis wins
    assert sgs['CPG40'].cram is None
    assert not sgs['CPG11'].active
    assert m.active_sg_ids['ds-one'] == ('CPG10', 'CPG12', 'CPG30')
    assert sgs['CPG30'].meta == {'x': {'y': 1, 'z': 2}, 'phenotypes': {'HP:1': 'yes'}}
    assert sgs['CPG30'].participant_id is None
    assert sgs['CPG30'].job_attrs()['participant_id'] == 'S30'  # falls back to the external ID

    assert m.previous_spicy_vcf == 'gs://a/new/fresh_ids.vcf.bgz'


def test_manifest_pedigree() -> None:
    sgs = manifest_for().sg_by_id()
    assert sgs['CPG10'].ped_fields() == ['FAM-1', 'CPG10', 'CPG12', 'CPG11', '1', '2']
    assert sgs['CPG11'].ped_fields() == ['FAM-1', 'CPG11', '0', '0', '2', '1']
    assert sgs['CPG12'].ped_fields() == ['FAM-1', 'CPG12', '0', '0', '1', '0']  # sex 'male', affected None
    assert sgs['CPG20'].ped_fields() == ['P20', 'CPG20', '0', '0', '1', '0']  # no PED row: reportedSex
    assert sgs['CPG30'].ped_fields() == ['S30', 'CPG30', '0', '0', '0', '0']
    assert sgs['CPG40'].ped_fields() == ['FAM2', 'CPG40', '0', '0', '0', '0']  # mother in another dataset


def test_manifest_round_trips() -> None:
    m = manifest_for()
    assert Manifest.from_json(m.to_json()) == m
    assert Manifest.from_json(m.to_json()).to_json() == m.to_json()


def test_manifest_main_namespace_and_output_version_override() -> None:
    m = manifest_for('main', output_version='My Version.1')
    assert m.output_version == 'my-version-1'
    assert m.sg_by_id()['CPG10'].cram == 'gs://cpg-ds-one-main/cram/CPG10.cram'


def test_inactive_cohort_is_refused() -> None:
    data = fixture()
    data['cohorts']['COH2']['status'] = 'archived'
    config = cfg.merge_layers(run_config(), dataset='analysis-ds', billing_project='bp', driver_image='img')
    with pytest.raises(MetamistError, match=r"Inactive Cohorts: \['COH2'\]"):
        build_manifest(ManifestRequest.from_config(config), Metamist(FakeMetamist(data).query))


def test_dataset_input_with_only_sgs() -> None:
    config = cfg.merge_layers(
        {'workflow': {'access_level': 'test', 'input_datasets': ['ds-one'], 'only_sgs': ['CPG30', 'CPG10']}},
        dataset='analysis-ds',
        billing_project='bp',
        driver_image='img',
    )
    m = build_manifest(ManifestRequest.from_config(config), metamist()[0])
    assert [sg.id for sg in m.sequencing_groups] == ['CPG10', 'CPG30']
    assert m.cohorts == ()
    assert m.name == 'analysis-ds'


def test_combined_ped_cleans_family_ids_and_appends_ref_panel() -> None:
    m = manifest_for()
    text = combined_ped(m.cohort_sequencing_groups('COH1'), REF_PED, 'COH1')
    assert text.splitlines()[:2] == ['P20\tCPG20\t0\t0\t1\t0', 'FAM_1\tCPG10\tCPG12\tCPG11\t1\t2']
    assert text.endswith(REF_PED)
    assert clean_ped_family_id('fam.1 x') == 'fam_1_x'
    assert clean_ped_family_id('Fam_1') == 'Fam_1'


# ---- make_plan / write_plan -----------------------------------------------------------------------------------


def make(entry: str, storage: DirStorage, **workflow: Any) -> tuple[planning.Plan, dict[str, Any]]:
    return planning.make_plan(
        entry=entry,
        run_config=run_config(**workflow),
        dataset='analysis-ds',
        billing_project='bp',
        driver_image='img',
        storage=storage,
        metamist=metamist()[0],
        gse_chunk_size=2,
    )


def manifest_and_config(storage: DirStorage) -> Manifest:
    """The fixture's manifest, with its resolved config active (multi_sample needs nothing to exist but the PED)."""
    storage.put(cfg.load_defaults()['references']['ped_file'], REF_PED)
    return make('multi_sample', storage)[0].manifest


def put_all(storage: DirStorage, uris: list[str]) -> None:
    for uri in uris:
        storage.put(uri, 'x')


def put_crams(storage: DirStorage, plan_manifest: Manifest) -> None:
    put_all(storage, [sg.cram for sg in plan_manifest.sequencing_groups if sg.cram])


def test_sample_evidence_needs_a_cram_for_every_sg(storage: DirStorage) -> None:
    with pytest.raises(planning.PlanError, match='no completed cram analysis in metamist: CPG40'):
        make('sample_evidence', storage)


def test_sample_evidence_plan(storage: DirStorage) -> None:
    from gatk_sv_nf.stages import gather_sample_evidence as gse

    # CPG40 has no CRAM, but its outputs exist, so it's skipped before the CRAM check
    manifest = manifest_and_config(storage)
    put_all(storage, list(gse.expected_outputs(manifest.sg_by_id()['CPG40']).values()))
    put_all(storage, list(gse.expected_outputs(manifest.sg_by_id()['CPG10']).values()))
    with pytest.raises(planning.PlanError, match='do not exist'):
        make('sample_evidence', storage)
    put_crams(storage, manifest)

    plan, config = make('sample_evidence', storage)
    assert plan.gse_chunks == [['CPG20', 'CPG11'], ['CPG12', 'CPG30']]
    assert plan.plan_dir.startswith('gs://cpg-analysis-ds-test-tmp/gatk_sv_nf/plans/')

    plan_json = plan.plan_json(config)
    assert plan_json == {
        'plan_dir': plan.plan_dir,
        'entry': 'sample_evidence',
        'output_version': plan.manifest.output_version,
        'cohort_ids': ['COH1', 'COH2'],
        'dataset_names': ['ds-two', 'ds-one'],
        'seqr_datasets': [],
        'previous_spicy_vcf': None,  # only looked up for multi_sample
        'gse_chunks': [f'{plan.plan_dir}/gse_chunks/chunk_0.json', f'{plan.plan_dir}/gse_chunks/chunk_1.json'],
        'images': {'bcftools': config['images']['bcftools'], 'strvctvre': config['images']['strvctvre']},
    }

    planning.write_plan(plan, storage)
    assert json.loads(storage.text(plan_json['gse_chunks'][1]) or '') == ['CPG12', 'CPG30']
    written = toml.loads(storage.text(f'{plan.plan_dir}/config.toml') or '')
    assert written['storage']['ds-two']['default'] == 'gs://cpg-ds-two-test'
    assert written['workflow']['dataset_gcp_project'] == 'bp'
    assert Manifest.from_json(storage.text(f'{plan.plan_dir}/manifest.json') or '') == plan.manifest

    # the stage reads the manifest it wrote
    from gatk_sv_nf.manifest import load_sequencing_groups

    assert list(load_sequencing_groups(storage.text(f'{plan.plan_dir}/manifest.json') or '')) == MC_ORDER

    # same inputs, same plan dir (arGuid is never an input); a changed config moves it
    assert make('sample_evidence', storage)[0].plan_dir == plan.plan_dir
    assert make('sample_evidence', storage, min_batch_size=11)[0].plan_dir != plan.plan_dir


def test_evidence_qc_requires_evidence(storage: DirStorage) -> None:
    from gatk_sv_nf.stages import gather_sample_evidence as gse

    with pytest.raises(planning.PlanError, match='6 SG'):
        make('evidence_qc', storage, min_batch_size=6)
    for sg in manifest_and_config(storage).sequencing_groups:
        put_all(storage, gse.evidence_qc_inputs(sg))
    plan, _ = make('evidence_qc', storage, min_batch_size=6)
    assert plan.gse_chunks == []


def test_evidence_qc_refuses_too_few_sgs(storage: DirStorage) -> None:
    """cpg-flow refuses while queueing CreateSampleBatches, before EvidenceQC runs; defaults.toml says 10."""
    from gatk_sv_nf.stages import create_sample_batches as csb

    with pytest.raises(planning.PlanError, match=r'6 sequencing group.*min_batch_size is 10'):
        make('evidence_qc', storage)
    # nothing is queued for a stage whose output exists, so then there's nothing to refuse
    manifest = manifest_and_config(storage)
    put_all(storage, list(csb.expected_outputs(manifest).values()))
    with pytest.raises(planning.PlanError, match='missing GatherSampleEvidence outputs'):
        make('evidence_qc', storage)
    with pytest.raises(planning.PlanError, match='min_batch_size'):
        make('evidence_qc', storage, check_expected_outputs=False)


def test_multi_sample_side_files(storage: DirStorage) -> None:
    with pytest.raises(planning.PlanError, match=r'references\.ped_file'):
        make('multi_sample', storage)
    ref_ped_uri = cfg.load_defaults()['references']['ped_file']
    storage.put(ref_ped_uri, REF_PED)

    plan, config = make('multi_sample', storage, write_seqr_vcf_for_datasets=['ds-one', 'not-in-run'])
    m, ov = plan.manifest, plan.manifest.output_version
    assert plan.plan_json(config)['seqr_datasets'] == ['ds-one']
    assert plan.plan_json(config)['previous_spicy_vcf'] == 'gs://a/new/fresh_ids.vcf.bgz'

    coh1_ped = 'gs://cpg-analysis-ds-test/gatk_sv/COH1/MakeCohortCombinedPed/ped_with_ref_panel.ped'
    mc_ped = f'gs://cpg-analysis-ds-test/gatk_sv/{ov}/MakeMultiCohortCombinedPed/ped_with_ref_panel.ped'
    assert paths.cohort_ped('analysis-ds', 'COH1') == coh1_ped
    files = plan.side_files
    assert [line.split('\t')[1] for line in files[coh1_ped].splitlines()] == [
        'CPG20',
        'CPG10',
        'CPG11',
        'CPG12',
        'HG00096',
    ]
    assert [line.split('\t')[1] for line in files[mc_ped].splitlines()] == [*MC_ORDER, 'HG00096']
    assert files['gs://cpg-ds-one-test-tmp/' + ov + '/sv-sgid-list.txt'] == 'CPG10\nCPG12\nCPG30\n'
    assert files[f'gs://cpg-analysis-ds-test-tmp/{ov}/mc-sv-sgid-list.txt'] == 'CPG10\nCPG12\nCPG20\nCPG30\nCPG40\n'
    assert set(files) == {
        coh1_ped,
        paths.cohort_ped('ds-one', 'COH2'),
        mc_ped,
        paths.dataset_sgid_list('ds-one', ov),
        paths.dataset_sgid_list('ds-two', ov),
        paths.multicohort_sgid_list('analysis-ds', ov),
    }
    assert m.cohort_ids == ['COH1', 'COH2']

    # an existing PED is kept, as cpg-flow skips a stage whose outputs exist
    storage.put(coh1_ped, 'old')
    plan, _ = make('multi_sample', storage)
    assert coh1_ped not in plan.side_files
    planning.write_plan(plan, storage)
    assert storage.text(coh1_ped) == 'old'
    assert storage.text(mc_ped) == files[mc_ped]


def test_multi_sample_refuses_overlapping_cohorts(storage: DirStorage) -> None:
    data = fixture()
    data['cohorts']['COH2']['sequencing_groups'].append('CPG10')
    with pytest.raises(ValueError, match=r"Overlapping cohorts COH1 and COH2 have overlapping SGs: \{'CPG10'\}"):
        planning.make_plan(
            entry='multi_sample',
            run_config=run_config(),
            dataset='analysis-ds',
            billing_project='bp',
            driver_image='img',
            storage=storage,
            metamist=Metamist(FakeMetamist(data).query),
        )


# ---- CLI ------------------------------------------------------------------------------------------------------


def cli_args(tmp_path: Path, **overrides: Any) -> argparse.Namespace:
    config_path = tmp_path / 'run.toml'
    config_path.write_text(toml.dumps(run_config(min_batch_size=1)))
    args = {
        'entry': 'evidence_qc',
        'config': str(config_path),
        'dataset': 'analysis-ds',
        'billing_project': 'bp',
        'driver_image': None,
        'gse_chunk_size': 50,
        'out': str(tmp_path / 'plan.json'),
        'dry_run': True,
        'print_config': False,
    }
    return argparse.Namespace(**(args | overrides))


def test_cli_dry_run_writes_nothing(tmp_path: Path, storage: DirStorage, capsys: pytest.CaptureFixture[str]) -> None:
    from gatk_sv_nf.stages import gather_sample_evidence as gse

    for sg in manifest_and_config(storage).sequencing_groups:
        put_all(storage, gse.evidence_qc_inputs(sg))
    before = sorted(p for p in storage.local.root.rglob('*'))

    code = cli.cmd_plan(cli_args(tmp_path), RunContext(storage=storage), metamist()[0])
    assert code == EXIT_OK
    printed = json.loads(capsys.readouterr().out)
    assert printed == json.loads((tmp_path / 'plan.json').read_text())
    assert printed['entry'] == 'evidence_qc'
    assert sorted(p for p in storage.local.root.rglob('*')) == before

    code = cli.cmd_plan(cli_args(tmp_path, print_config=True), RunContext(storage=storage), metamist()[0])
    assert code == EXIT_OK
    resolved = toml.loads(capsys.readouterr().out)
    assert resolved['workflow']['driver_image'] == 'unset'
    assert 'ds-one' in resolved['storage']


def test_cli_reports_config_errors(tmp_path: Path, storage: DirStorage, caplog: pytest.LogCaptureFixture) -> None:
    args = cli_args(tmp_path, entry='sample_evidence')
    Path(args.config).write_text(toml.dumps(run_config(permit_inactive_cohorts=True)))
    assert cli.cmd_plan(args, RunContext(storage=storage), metamist()[0]) == EXIT_USAGE
    assert 'should not tolerate inactive' in caplog.text
