"""Summary statistics QC, as Open Targets runs it before fine-mapping."""

from __future__ import annotations

from gentropy.common.session import Session
from gentropy.dataset.summary_statistics import SummaryStatistics
from gentropy.dataset.summary_statistics_qc import SummaryStatisticsQC

# `StudyIndex.annotate_sumstats_qc` defaults, for GWAS on genotyping arrays.
QC_THRESHOLDS = {
    "mean_beta": "|value| <= 0.05",
    "mean_diff_pz": "|value| <= 0.05",
    "se_diff_pz": "value <= 0.05",
    "gc_lambda": "0.7 <= value <= 2.5",
    "n_variants": "value >= 2,000,000",
}


def run_sumstats_qc(session: Session, sumstats_path: str, output_path: str) -> dict[str, float]:
    """Compute the summary statistics QC metrics of each study.

    This is gentropy's `summary_statistics_qc` step: mean beta, the PZ check (how well the
    reported p-values agree with z = beta/SE), genomic control lambda and the number of
    variants. `fmg run --sumstats-qc` turns them into study index flags.

    Args:
        session (Session): Session.
        sumstats_path (str): gentropy summary statistics (`fmg ingest` output).
        output_path (str): Where to write the metrics, one row per study.

    Returns:
        dict[str, float]: The metrics of the first study, by name.
    """
    sumstats = SummaryStatistics.from_parquet(session, sumstats_path)
    SummaryStatisticsQC.from_summary_statistics(gwas=sumstats).df.repartition(1).write.mode(
        session.write_mode
    ).parquet(output_path)
    row = session.spark.read.parquet(output_path).first()
    return {k: v for k, v in row.asDict().items() if k != "studyId"}
