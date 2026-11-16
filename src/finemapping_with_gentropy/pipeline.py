"""Locus breaker clumping -> SuSiE-inf per locus -> credible set QC."""

from __future__ import annotations

import logging
import time
from pathlib import Path

import pandas as pd
import pyspark.sql.functions as f
from gentropy.common.session import Session
from gentropy.credible_set_qc import CredibleSetQCStep
from gentropy.locus_breaker_clumping import LocusBreakerClumpingStep
from gentropy.susie_finemapper import SusieFineMapperStep
from pyspark.sql import types as t

from finemapping_with_gentropy.config import CLUMPING_PARAMS, CS_QC_PARAMS, SUSIE_PARAMS

logger = logging.getLogger(__name__)


def write_study_index(
    session: Session, path: str, study_id: str, n_samples: int, ld_population: str
) -> None:
    """Write the smallest study index the fine-mapper accepts.

    The fine-mapper reads the LD population and sample size from the study index, and skips
    the study unless it is a GWAS with summary statistics, at least 90% of its samples from
    nfe, csa or afr, and no failing QC or analysis flag.

    Args:
        session (Session): Session.
        path (str): Where to write the study index.
        study_id (str): Study identifier, as in the summary statistics.
        n_samples (int): Sample size; SuSiE-inf uses it as `n`.
        ld_population (str): gentropy LD population of the study: nfe, csa or afr.
    """
    schema = t.StructType(
        [
            t.StructField("studyId", t.StringType(), False),
            t.StructField("projectId", t.StringType(), False),
            t.StructField("studyType", t.StringType(), False),
            t.StructField("nSamples", t.IntegerType(), True),
            t.StructField(
                "ldPopulationStructure",
                t.ArrayType(
                    t.StructType(
                        [
                            t.StructField("ldPopulation", t.StringType(), True),
                            t.StructField("relativeSampleSize", t.DoubleType(), True),
                        ]
                    )
                ),
                True,
            ),
            t.StructField("hasSumstats", t.BooleanType(), True),
            t.StructField("qualityControls", t.ArrayType(t.StringType()), True),
            t.StructField("analysisFlags", t.ArrayType(t.StringType()), True),
        ]
    )
    row = (study_id, "external", "gwas", n_samples, [(ld_population, 1.0)], True, [], [])
    session.spark.createDataFrame([row], schema).write.mode("overwrite").parquet(path)


def clump(session: Session, sumstats_path: str, clumped_path: str) -> int:
    """Locus breaker clumping, one partition directory per locus.

    Args:
        session (Session): Session.
        sumstats_path (str): gentropy summary statistics.
        clumped_path (str): Where to write the loci, partitioned by `studyLocusId`.

    Returns:
        int: Number of loci.
    """
    LocusBreakerClumpingStep(
        session=session,
        summary_statistics_input_path=sumstats_path,
        clumped_study_locus_output_path=clumped_path,
        **CLUMPING_PARAMS,
    )
    return session.spark.read.parquet(clumped_path).count()


def select_loci(
    session: Session, clumped_path: str, regions: list[str] | None, max_loci: int | None
) -> list[str]:
    """Pick the loci to fine-map.

    Args:
        session (Session): Session.
        clumped_path (str): Clumped loci.
        regions (list[str] | None): `chr:start-end` regions; keeps loci whose lead is inside one.
        max_loci (int | None): Keep at most this many loci.

    Returns:
        list[str]: `studyLocusId`s, sorted.
    """
    loci = session.spark.read.parquet(clumped_path)
    if regions:
        condition = f.lit(False)
        for region in regions:
            chromosome, bounds = region.split(":")
            start, end = (int(x) for x in bounds.split("-"))
            condition = condition | (
                (f.col("chromosome") == chromosome) & f.col("position").between(start, end)
            )
        loci = loci.filter(condition)
    ids = sorted(row.studyLocusId for row in loci.select("studyLocusId").collect())
    return ids[:max_loci] if max_loci else ids


def finemap(
    session: Session,
    clumped_path: str,
    loci: list[str],
    study_index_path: str,
    ld_matrix_paths: dict[str, str],
    out: Path,
) -> None:
    """Run SuSiE-inf on each locus, as one production Batch task per locus does.

    Args:
        session (Session): Session able to read the LD matrices.
        clumped_path (str): Clumped loci.
        loci (list[str]): `studyLocusId`s to fine-map.
        study_index_path (str): Study index.
        ld_matrix_paths (dict[str, str]): LD resources.
        out (Path): Output directory.
    """
    # A `studyLocusId=<x>` partition directory holds no `studyLocusId` column; write each
    # locus on its own with the column, so the fine-mapper reads a complete study locus.
    clumped = session.spark.read.parquet(clumped_path)
    inputs = out / "study_locus_inputs"
    for locus in loci:
        clumped.filter(f.col("studyLocusId") == locus).coalesce(1).write.mode("overwrite").parquet(
            str(inputs / locus)
        )

    (out / "logs").mkdir(parents=True, exist_ok=True)
    manifest_path = out / "manifest.csv"
    pd.DataFrame(
        {
            "study_locus_input": [str(inputs / locus) for locus in loci],
            "study_locus_output": [str(out / "credible_sets" / locus) for locus in loci],
            "log_output": [str(out / "logs" / locus) for locus in loci],
        }
    ).to_csv(manifest_path, index=False)

    for idx, locus in enumerate(loci):
        t0 = time.time()
        try:
            SusieFineMapperStep(
                session=session,
                study_index_path=study_index_path,
                study_locus_manifest_path=str(manifest_path),
                study_locus_index=idx,
                ld_matrix_paths=ld_matrix_paths,
                **SUSIE_PARAMS,
            )
            status = "ok"
        except Exception as e:  # keep going: a failing locus should not stop the study
            status = f"FAILED: {type(e).__name__}: {e}"[:300]
        logger.warning("[%d/%d] %s %.0fs %s", idx + 1, len(loci), locus, time.time() - t0, status)


def credible_set_qc(session: Session, out: Path) -> int:
    """Gather the credible sets of every locus, filter them and drop duplicates.

    Overlapping locus-breaker windows can fine-map the same lead twice; one credible set per
    `studyLocusId` (study, lead and method) is kept, the one with the highest log10 BF.

    Args:
        session (Session): Session.
        out (Path): Output directory holding `credible_sets/`.

    Returns:
        int: Number of credible sets kept.
    """
    CredibleSetQCStep(
        session=session,
        credible_sets_path=str(out / "credible_sets"),
        output_path=str(out / "credible_sets_clean"),
        ld_index_path=None,  # no LD index: no LD clumping, as in production
        study_index_path=None,
        ld_min_r2=None,
        n_partitions=1,
        **CS_QC_PARAMS,
    )
    return session.spark.read.parquet(str(out / "credible_sets_clean")).count()
