"""Convert GWAS-SSF harmonised summary statistics to gentropy `SummaryStatistics`."""

from __future__ import annotations

import pyspark.sql.functions as f
from gentropy.common.session import Session
from gentropy.datasource.gwas_catalog.summary_statistics import GWASCatalogSummaryStatistics


def ingest_sumstats(
    session: Session, sumstats_file: str, output_path: str, study_id: str | None = None
) -> int:
    """Parse a GWAS Catalog harmonised (GWAS-SSF, GRCh38) file as Open Targets does.

    This is the `gwas_catalog_sumstat_preprocess` step: parse, then `sanity_filter`, which
    drops variants with a missing, zero or infinite effect, a non-positive standard error, a
    p-value of 1 or more, and duplicated variants.

    Args:
        session (Session): Session.
        sumstats_file (str): `*.h.tsv.gz` file, with the `hm_*` columns or the GWAS-SSF ones.
        output_path (str): Where to write the parquet summary statistics.
        study_id (str | None): Study identifier; by default parsed from a `GCST` file name.

    Returns:
        int: Number of variants written.
    """
    sumstats = GWASCatalogSummaryStatistics.from_gwas_harmonized_summary_stats(
        session.spark, sumstats_file
    ).sanity_filter()
    df = sumstats.df
    if study_id is not None:
        df = df.withColumn("studyId", f.lit(study_id))
    df.repartition(8).write.mode(session.write_mode).parquet(output_path)
    return session.spark.read.parquet(output_path).count()
