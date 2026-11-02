"""Fixtures for gatk_sv_nf: a resolved config, plus cpg-cromwell's fake Cromwell and directory-backed GCS."""

import sys
from collections.abc import Iterator
from pathlib import Path

import pytest

# reuse cpg-cromwell's test doubles rather than copying them
sys.path.insert(0, str(Path(__file__).parents[1] / 'cpg-cromwell'))
from tests.conftest import DirStorage, FakeCromwell, cromwell, storage  # noqa: F401

COMMIT = '890582922bccb4b651275506a34311c949417f6c'
FASTA = 'gs://cpg-common-main/references/hg38/Homo_sapiens_assembly38_masked.fasta'


def _storage(ds: str) -> str:
    return f"""
[storage.{ds}]
default = "gs://cpg-{ds}-test"
analysis = "gs://cpg-{ds}-test-analysis"
tmp = "gs://cpg-{ds}-test-tmp"
web = "gs://cpg-{ds}-test-web"
web_url = "https://test-web.populationgenomics.org.au/{ds}"
"""


BASE_CONFIG = (
    f"""
[workflow]
dataset = "analysis-ds"
access_level = "test"
dataset_gcp_project = "analysis-ds-project"
sequencing_type = "genome"
check_expected_outputs = true
gatk_sv_commit = "{COMMIT}"
ref_fasta = "{FASTA}"
time_limit_seconds = 86400

[images]
cloud_sdk_docker = "img/cloud_sdk"
gatk_docker = "img/gatk"
gatk_docker_pesr_override = "img/gatk_pesr"
genomes_in_the_cloud_docker = "img/gitc"
manta_docker = "img/manta"
samtools_cloud_docker = "img/samtools"
scramble_docker = "img/scramble"
sv_base_mini_docker = "img/sv_base_mini"
sv_pipeline_docker = "img/sv_pipeline"
wham_docker = "img/wham"

[references]
manta_region_bed = "gs://ref/manta.bed.gz"
manta_region_bed_index = "gs://ref/manta.bed.gz.tbi"
mei_bed = "gs://ref/mei.bed"
preprocessed_intervals = "gs://ref/intervals.list"
primary_contigs_fai = "gs://ref/contigs.fai"
primary_contigs_list = "gs://ref/contigs.list"
dbsnp_vcf = "gs://ref/dbsnp.vcf"
wham_include_list_bed_file = "gs://ref/wham.bed"

[resource_overrides.GatherSampleEvidence.runtime_attr_wham]
mem_gb = 32

[cromwell_polling_intervals.large]
min = 0.01
max = 0.05
"""
    + _storage('default')
    + _storage('analysis-ds')
    + _storage('fixture-ds')
)


@pytest.fixture
def write_config(tmp_path: Path) -> Iterator:
    """Call with extra TOML to append; returns the config path. Resets cpg_utils' cache afterwards."""
    from cpg_utils import config as cpg_config_module

    def write(extra: str = '') -> Path:
        path = tmp_path / 'config.toml'
        path.write_text(BASE_CONFIG + extra)
        cpg_config_module.set_config_paths([str(path)])
        return path

    yield write
    cpg_config_module._config_paths = []
    cpg_config_module._config = None


@pytest.fixture
def config(write_config) -> Path:
    return write_config()
