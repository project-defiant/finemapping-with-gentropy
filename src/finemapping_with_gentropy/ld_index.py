"""Build the allele-aligned Pan-UKBB LD index from the public release."""

from __future__ import annotations

import pyspark.sql.functions as f
from gentropy.common.session import Session
from gentropy.datasource.pan_ukbb_ld.ld import PanUKBBLDMatrix

from finemapping_with_gentropy.config import PAN_UKBB_S3, aligned_index_path

# gentropy LD population label -> Pan-UKBB population label.
PAN_UKBB_POPULATIONS = {"nfe": "EUR", "csa": "CSA", "afr": "AFR"}


def build_ld_index(
    session: Session,
    sumstats_path: str,
    ld_index_dir: str,
    ld_population: str = "nfe",
    variant_table: str = f"{PAN_UKBB_S3}/UKBB.{{POP}}.ldadj.variant.b38.ht",
) -> int:
    """Align the Pan-UKBB GRCh38 variant index to the alleles of the summary statistics.

    Open Targets aligns the Pan-UKBB variant index once, to its variant annotation, and keeps
    the result on a private bucket (`gs://panukbb-ld-matrixes/UKBB.{POP}.aligned.parquet`).
    Here the summary statistics stand in for the variant annotation: `alleleOrder` is -1 where
    Pan-UKBB lists the study's alleles the other way round, and the LD of those variants is
    sign-flipped when the matrix is read. Panel variants absent from the study are kept with
    their Pan-UKBB alleles, so the index covers the whole panel (23.9M variants for EUR) and
    serves any study whose alleles agree with it.

    Args:
        session (Session): Session able to read the public Pan-UKBB bucket.
        sumstats_path (str): gentropy summary statistics (`fmg ingest` output).
        ld_index_dir (str): Directory to write the index to.
        ld_population (str): gentropy LD population: nfe, csa or afr.
        variant_table (str): Pan-UKBB GRCh38 variant Hail table, `{POP}` for the population.
            Reading it from S3 takes hours (56,762 small objects read one by one); a local copy
            from `scripts/download_pan_ukbb_index.sh` takes minutes.

    Returns:
        int: Number of study variants found in the Pan-UKBB index.
    """
    population = PAN_UKBB_POPULATIONS[ld_population]
    alleles = f.split("variantId", "_")
    variants = session.spark.read.parquet(sumstats_path).select(
        "chromosome",
        "position",
        alleles.getItem(2).alias("referenceAllele"),
        alleles.getItem(3).alias("alternateAllele"),
    )
    PanUKBBLDMatrix().align_ld_index_alleles(
        variant_annotation=variants,
        population=population,
        hail_table_path=variant_table,
        hail_table_output=aligned_index_path(ld_index_dir),
    )
    return session.spark.read.parquet(
        aligned_index_path(ld_index_dir).format(POP=population)
    ).count()
