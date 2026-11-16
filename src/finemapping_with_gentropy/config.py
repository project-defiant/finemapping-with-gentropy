"""Parameters and data locations.

The method parameters mirror the Open Targets production configuration
(opentargets/pipeline, orchestration):

- clumping:     dags/config/gwas_catalog_sumstats_susie_clumping.yaml
- fine-mapping: assets/susie_finemap.sh
- QC:           dags/config/credible_set_qc.yaml
"""

from __future__ import annotations

from enum import StrEnum

# Locus-breaker clumping.
CLUMPING_PARAMS = dict(
    lbc_baseline_pvalue=1.0e-5,
    lbc_distance_cutoff=250_000,
    lbc_pvalue_threshold=1.0e-8,
    lbc_flanking_distance=100_000,
    large_loci_size=1_500_000,
    wbc_clump_distance=500_000,
    wbc_pvalue_threshold=1.0e-8,
    collect_locus=True,
    remove_mhc=True,
)

# SuSiE-inf fine-mapping of one locus.
SUSIE_PARAMS = dict(
    max_causal_snps=10,
    lead_pval_threshold=1e-5,
    purity_mean_r2_threshold=0.25,
    purity_min_r2_threshold=0.25,
    cs_lbf_thr=2,
    sum_pips=0.95,
    susie_est_tausq=False,
    run_carma=False,
    run_sumstat_imputation=False,
    carma_time_limit=600,
    imputed_r2_threshold=0.9,
    ld_score_threshold=5,
    carma_tau=0.15,
    ld_min_r2=0.8,
)

# Credible set QC over all loci of the study.
CS_QC_PARAMS = dict(
    p_value_threshold=1.0e-5,
    purity_min_r2=0.25,
    clump=False,
)


class LDSource(StrEnum):
    """Where the Pan-UKBB LD matrices are read from."""

    PUBLIC = "public"
    """Pan-UKBB release on AWS (s3://pan-ukb-us-east-1), read anonymously."""
    OPENTARGETS = "opentargets"
    """Open Targets copy on GCS (gs://panukbb-ld-matrixes); needs Open Targets credentials."""


PAN_UKBB_S3 = "s3a://pan-ukb-us-east-1/ld_release"

# gnomAD v2.1.1 LD is only used for populations other than nfe/csa/afr; it is public on GCS.
_GNOMAD_PATHS = {
    "ld_matrix_template": "gs://gcp-public-data--gnomad/release/2.1.1/ld/gnomad.genomes.r2.1.1.{POP}.common.adj.ld.bm",
    "ld_index_raw_template": "gs://gcp-public-data--gnomad/release/2.1.1/ld/gnomad.genomes.r2.1.1.{POP}.common.ld.variant_indices.ht",
    "liftover_ht_path": "gs://gcp-public-data--gnomad/release/2.1.1/liftover_grch38/ht/genomes/gnomad.genomes.r2.1.1.sites.liftover_grch38.ht",
    "grch37_to_grch38_chain_path": "gs://hail-common/references/grch37_to_grch38.over.chain.gz",
}


def ld_matrix_paths(source: LDSource, ld_index_dir: str | None = None) -> dict[str, str]:
    """LD resources for `SusieFineMapperStep`.

    Args:
        source (LDSource): Where to read the Pan-UKBB LD matrices from.
        ld_index_dir (str | None): Directory with the aligned LD index built by `fmg ld-index`;
            required for the public source.

    Returns:
        dict[str, str]: `ld_matrix_paths` with a `{POP}` placeholder for the population.

    Raises:
        ValueError: When the public source is used without an LD index directory.
    """
    if source is LDSource.OPENTARGETS:
        return {
            "pan_ukbb_bm_path": "gs://panukbb-ld-matrixes/UKBB.{POP}.ldadj",
            "pan_ukbb_ht_path": "gs://panukbb-ld-matrixes/ukb-diverse-pops-public-build-38/UKBB.{POP}.ldadj.variant.b38",
            "ukbb_annotation_path": "gs://panukbb-ld-matrixes/UKBB.{POP}.aligned.parquet",
            **_GNOMAD_PATHS,
        }
    if ld_index_dir is None:
        raise ValueError("The public LD source needs the LD index built by `fmg ld-index`.")
    return {
        "pan_ukbb_bm_path": f"{PAN_UKBB_S3}/UKBB.{{POP}}.ldadj.bm",
        "pan_ukbb_ht_path": f"{PAN_UKBB_S3}/UKBB.{{POP}}.ldadj.variant.b38.ht",
        "ukbb_annotation_path": aligned_index_path(ld_index_dir),
        **_GNOMAD_PATHS,
    }


def aligned_index_path(ld_index_dir: str) -> str:
    """Template of the aligned Pan-UKBB LD index written by `fmg ld-index`.

    Args:
        ld_index_dir (str): Directory holding the index.

    Returns:
        str: Path template with a `{POP}` placeholder.
    """
    return f"{ld_index_dir}/UKBB.{{POP}}.aligned.parquet"
