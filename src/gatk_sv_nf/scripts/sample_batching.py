# source: cpg-flow-gatk-sv src/cpg_flow_gatk_sv/scripts/sample_batching.py @ 0824ad3e819d233b6177c31c7de31e703e16ec5e
# Keep in sync until cutover (NEXTFLOW_PORT_PLAN.md §8): everything below this header is the source, unchanged.
"""
extract from Broad sample batching script for GATK-SV
"""

import json
from argparse import ArgumentParser

import numpy as np
import pandas as pd
from loguru import logger

from cpg_utils import config, to_path

SEX_VALS = {'male', 'female'}


def batch_sgs(md: pd.DataFrame, min_batch_size: int, max_batch_size: int) -> list[dict]:
    """
    Batch sequencing groups by coverage, and chrX ploidy

    Starts with a base assumption of one batch, increasing the batch count
    until min < batch size < max

    When an appropriate number of batches is found, the SGs are split
    by sex (assigned by inferred X ploidy), then all SGs are ordered
    by median coverage.

    Using numpy.array_split, the ordered male & female sample lists are split
    into equal sized sub-lists, then the sub-lists are combined into batches.
    This groups lowest coverage male and female samples into a single batch,
    continuing up to the highest coverage batch.

    This method maintains a consistent sex ratio across all batches

    Args:
        md (str): DataFrame of metadata
        min_batch_size (int): minimum batch size
        max_batch_size (int): maximum batch size

    Returns:
        A list of batches, each with sequencing_groups
        Each batch self-documents its size and male/female ratio
        {
            batch_ID: {
                'size': int,
                'male_count': int,
                'female_count': int,
                'mf_ratio': float,
                'batch_median_coverage': float,
                'coverage_range': (float, float),
                'sequencing_groups': [sample1, sample2, ...],
                'coverage_medians': [float, float, ...],
                'male': [sample1, sample3, ...],
                'female': [sample1, sample3, ...],
            }
        }
    """

    # Split SGs based on >= 2 copies of chrX vs. < 2 copies
    is_female = md.chrX_CopyNumber_rounded >= 2
    is_male = md.chrX_CopyNumber_rounded == 1

    # region: Calculate approximate number of batches
    n_sg = len(md)

    # shortcut return if the total sequencing groups is a valid batch
    if min_batch_size <= n_sg <= max_batch_size:
        logger.info(f'Number of sequencing_groups ({n_sg}) is within range of batch sizes')
        return [
            {
                'size': n_sg,
                'male_count': len(md[~is_female]),
                'female_count': len(md[is_female]),
                'mf_ratio': is_male.sum() / is_female.sum(),
                'batch_median_coverage': md.median_coverage.median(),
                'coverage_range': (md.median_coverage.min(), md.median_coverage.max()),
                'sequencing_groups': md.sample_id.tolist(),
                'coverage_medians': md.median_coverage.tolist(),
                'male': md[~is_female].sample_id.to_list(),
                'female': md[is_female].sample_id.to_list(),
            },
        ]

    # start with a single bin and work upwards
    cov_bins = 1
    n_per_batch = n_sg // cov_bins

    # add more bins until we get to a reasonable number of SGs per batch
    # shrink in one direction by increasing num. batches
    while n_per_batch > max_batch_size:
        cov_bins += 1
        n_per_batch = n_sg // cov_bins
    # endregion

    logger.info(
        f"""
    Batching Specs:
    Total sequencing_groups: {n_sg}
    Num. bins: {cov_bins}
    Approx sequencing groups per batch: {n_per_batch}
""",
    )

    # Split sequencing groups by sex, then roughly by coverage
    md_sex = {
        'male': md[~is_female].sort_values(by='median_coverage'),
        'female': md[is_female].sort_values(by='median_coverage'),
    }

    logger.info('Sex distribution across all samples:')
    for sex, entries in md_sex.items():
        logger.info(f'{sex}: {len(entries)}')

    # array split each sex proportionally
    md_sex_cov = {sex: np.array_split(md_sex[sex], cov_bins) for sex in SEX_VALS}

    # create batches
    batches: list[dict] = []
    for cov in range(cov_bins):
        sample_ids: pd.Series[str] = pd.concat([md_sex_cov['male'][cov], md_sex_cov['female'][cov]])  # type: ignore[list-item]
        logger.info(
            f"""
        ---
        Batch {cov}:
        Samples: {len(sample_ids)}
        Males: {len(md_sex_cov['male'][cov])}
        Females: {len(md_sex_cov['female'][cov])}
        ---
        """,
        )
        batches.append(
            {
                'size': len(sample_ids),
                'male_count': len(md_sex_cov['male'][cov]),
                'female_count': len(md_sex_cov['female'][cov]),
                'mf_ratio': (len(md_sex_cov['male'][cov]) or 1) / (len(md_sex_cov['female'][cov]) or 1),
                'batch_median_coverage': sample_ids.median_coverage.median(),
                'coverage_range': (sample_ids.median_coverage.min(), sample_ids.median_coverage.max()),
                'sequencing_groups': sample_ids.sample_id.tolist(),
                'coverage_medians': sample_ids.median_coverage.tolist(),
                'male': md_sex_cov['male'][cov].sample_id.tolist(),
                'female': md_sex_cov['female'][cov].sample_id.tolist(),
            },
        )

    return batches


def batch_sgs_by_library(md: pd.DataFrame, min_batch_size: int, max_batch_size: int) -> list[dict]:
    """
    Batch sequencing groups by coverage, chrX ploidy, and sequencing library

    Args:
        md (pd.DataFrame): DataFrame of metadata
        min_batch_size (int): minimum batch size
        max_batch_size (int): maximum batch size

    Returns:
        A list of batches, each with sequencing_groups and additional information
    """
    # Check that the library column exists
    if 'library' not in md.columns:
        return batch_sgs(md, min_batch_size, max_batch_size)

    # Group SGs by library type
    library_groups = md.groupby('library')

    # Create batches from each library's DataFrame
    batches = []
    for _, library_df in library_groups:
        batches.extend(batch_sgs(library_df, min_batch_size, max_batch_size))

    return batches


def add_sg_meta_fields(sg_df: pd.DataFrame, sg_meta: dict[str, dict]) -> pd.DataFrame:
    """
    Adds the sg meta fields 'library' and 'facility' to the DataFrame

    Args:
        sg_df (pd.DataFrame): DataFrame of sequencing groups
        sg_meta (dict[str, dict]): meta dict keyed by SG sample_id

    Returns:
        pd.DataFrame: DataFrame with the sg_meta fields parsed and added
    """
    sg_df['library'] = sg_df['sample_id'].map(
        lambda x: sg_meta[x].get('library_type', sg_meta[x].get('sequencing_library', 'unknown')),
    )
    sg_df['facility'] = sg_df['sample_id'].map(
        lambda x: sg_meta[x].get('facility', sg_meta[x].get('sequencing_facility', 'unknown')),
    )
    return sg_df


def partition_batches(
    metadata_files: list[str],
    sequencing_groups_json: str,
    output_json: str,
):
    """
    Runs this process
    - loads the input data
    - obtains batches
    - writes out the data into one single cross-batch file
    - also writes out per-batch sample lists

    Args:
        metadata_files (list[str]): paths to the metadata files
        sequencing_groups_json (str): path to json of all SGs with their meta
        output_json (str): location to write the batch result
    """

    min_batch_size = config.config_retrieve(['workflow', 'min_batch_size'], 100)
    max_batch_size = config.config_retrieve(['workflow', 'max_batch_size'], 300)

    # read in the metadata contents
    logger.info('Starting the batch creation process')

    # load in the metadata files
    md = pd.concat([pd.read_csv(md_file, sep='\t', low_memory=False) for md_file in metadata_files])
    md.columns = [x.replace('#', '') for x in md.columns]

    # load the sequencing groups
    with to_path(sequencing_groups_json).open('r') as f:
        sequencing_groups = json.load(f)

    # filter to the PCR-state SGs we're interested in
    sample_ids = list(sequencing_groups.keys())  # noqa: F841
    md = md.query('sample_id in @sample_ids')
    md = add_sg_meta_fields(md, sequencing_groups)

    # check that we have enough samples to batch
    # should have already been checked prior to Stage starting
    if len(md) < min_batch_size:
        raise ValueError('Insufficient Seq Groups found for batch generation')

    # generate the batches
    batches = batch_sgs_by_library(md=md, min_batch_size=min_batch_size, max_batch_size=max_batch_size)

    # write out the batches to GCP
    logger.info(batches)

    # write the batch JSON out to a file, inc. PCR state
    with to_path(output_json).open('w') as handle:
        json.dump(batches, handle, indent=4)


if __name__ == '__main__':
    parser = ArgumentParser()
    parser.add_argument('--qc_tables', type=str, nargs='+', required=True)
    parser.add_argument('--sgs_json', type=str, required=True)
    parser.add_argument('--output_json', type=str, required=True)
    args = parser.parse_args()
    partition_batches(
        metadata_files=args.qc_tables,
        sequencing_groups_json=args.sgs_json,
        output_json=args.output_json,
    )
