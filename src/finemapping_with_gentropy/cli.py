"""`fmg` command line: ingest -> qc -> ld-index -> run."""

from __future__ import annotations

import argparse
import logging
import time
from pathlib import Path

from finemapping_with_gentropy.config import PAN_UKBB_S3, LDSource, ld_matrix_paths

logger = logging.getLogger("fmg")


def _ingest(args: argparse.Namespace) -> None:
    from finemapping_with_gentropy.ingest import ingest_sumstats
    from finemapping_with_gentropy.session import make_session

    session = make_session(LDSource.PUBLIC, args.driver_memory)
    n = ingest_sumstats(session, str(Path(args.input).resolve()), _abs(args.output), args.study_id)
    logger.warning("%d variants written to %s", n, args.output)


def _qc(args: argparse.Namespace) -> None:
    from finemapping_with_gentropy.qc import QC_THRESHOLDS, run_sumstats_qc
    from finemapping_with_gentropy.session import make_session

    session = make_session(LDSource.PUBLIC, args.driver_memory)
    t0 = time.time()
    metrics = run_sumstats_qc(session, _abs(args.sumstats), _abs(args.output))
    logger.warning("Summary statistics QC (%.0fs), production thresholds:", time.time() - t0)
    for name, value in metrics.items():
        logger.warning("  %-13s %-14.4g pass if %s", name, value, QC_THRESHOLDS.get(name, "-"))


def _ld_index(args: argparse.Namespace) -> None:
    from finemapping_with_gentropy.ld_index import build_ld_index
    from finemapping_with_gentropy.session import make_session

    session = make_session(LDSource.PUBLIC, args.driver_memory)
    t0 = time.time()
    n = build_ld_index(
        session,
        _abs(args.sumstats),
        _abs(args.output),
        args.ld_population,
        _abs(args.variant_table),
    )
    logger.warning("%d variants in the LD index (%.0fs)", n, time.time() - t0)


def _run(args: argparse.Namespace) -> None:
    from finemapping_with_gentropy import pipeline
    from finemapping_with_gentropy.session import make_session

    source = LDSource(args.ld_source)
    paths = ld_matrix_paths(source, _abs(args.ld_index) if args.ld_index else None)
    out = Path(args.out).resolve()
    out.mkdir(parents=True, exist_ok=True)
    clumped_path = str(out / "study_locus_lb_clumped")
    study_index_path = str(out / "study_index")
    session = make_session(source, args.driver_memory)

    if not args.credible_set_qc_only:
        sumstats_qc = None if args.no_sumstats_qc else _abs(args.sumstats_qc)
        if sumstats_qc is None:
            logger.warning("No summary statistics QC: the study is assumed to pass it")
        flags = pipeline.write_study_index(
            session,
            study_index_path,
            args.study_id,
            args.n_samples,
            args.ld_population,
            sumstats_qc,
        )
        if flags:
            # The fine-mapper would skip every locus of the study for these flags.
            logger.error("Study %s fails QC, not fine-mapped: %s", args.study_id, flags)
            raise SystemExit(1)
        if not args.skip_clumping:
            t0 = time.time()
            n = pipeline.clump(session, _abs(args.sumstats), clumped_path)
            logger.warning("Locus breaker: %d loci (%.0fs)", n, time.time() - t0)
        loci = pipeline.select_loci(session, clumped_path, args.regions, args.max_loci)
        logger.warning("Fine-mapping %d loci", len(loci))
        pipeline.finemap(session, clumped_path, loci, study_index_path, paths, out)

    t0 = time.time()
    n = pipeline.credible_set_qc(session, out)
    logger.warning("Credible set QC: %d credible sets kept (%.0fs)", n, time.time() - t0)


def _abs(path: str) -> str:
    return path if "://" in path else str(Path(path).resolve())


def main() -> None:
    """Entry point of `fmg`."""
    parser = argparse.ArgumentParser(prog="fmg", description=__doc__)
    parser.add_argument("--driver-memory", default="12g")
    sub = parser.add_subparsers(required=True)

    p = sub.add_parser("ingest", help="GWAS-SSF harmonised .h.tsv.gz -> gentropy parquet")
    p.add_argument("--input", required=True)
    p.add_argument("--output", required=True)
    p.add_argument("--study-id", help="default: parsed from the GCST file name")
    p.set_defaults(func=_ingest)

    p = sub.add_parser("qc", help="summary statistics QC: mean beta, PZ, GC lambda, n variants")
    p.add_argument("--sumstats", required=True)
    p.add_argument("--output", required=True)
    p.set_defaults(func=_qc)

    p = sub.add_parser("ld-index", help="align the public Pan-UKBB LD index to the sumstats")
    p.add_argument("--sumstats", required=True)
    p.add_argument("--output", required=True)
    p.add_argument("--ld-population", default="nfe", choices=["nfe", "csa", "afr"])
    p.add_argument(
        "--variant-table",
        default=f"{PAN_UKBB_S3}/UKBB.{{POP}}.ldadj.variant.b38.ht",
        help="Pan-UKBB GRCh38 variant table ({POP} = EUR/CSA/AFR); use a local copy, S3 is slow",
    )
    p.set_defaults(func=_ld_index)

    p = sub.add_parser("run", help="locus breaker -> SuSiE-inf -> credible set QC")
    p.add_argument("--sumstats", required=True)
    p.add_argument("--study-id", required=True)
    p.add_argument("--n-samples", type=int, required=True)
    p.add_argument("--ld-population", default="nfe", choices=["nfe", "csa", "afr"])
    p.add_argument("--ld-source", default="public", choices=[s.value for s in LDSource])
    p.add_argument("--ld-index", help="`fmg ld-index` output; required with --ld-source public")
    p.add_argument("--out", required=True)
    p.add_argument("--regions", nargs="+", help="only loci with a lead in chr:start-end")
    p.add_argument("--max-loci", type=int)
    p.add_argument("--skip-clumping", action="store_true", help="reuse the clumped loci")
    qc = p.add_mutually_exclusive_group()
    qc.add_argument("--sumstats-qc", help="`fmg qc` output; failing studies are not fine-mapped")
    qc.add_argument(
        "--no-sumstats-qc", action="store_true", help="fine-map without summary statistics QC"
    )
    p.add_argument("--credible-set-qc-only", action="store_true", help="only rerun credible set QC")
    p.set_defaults(func=_run)

    args = parser.parse_args()
    if (
        args.func is _run
        and not args.credible_set_qc_only
        and not (args.sumstats_qc or args.no_sumstats_qc)
    ):
        parser.error("run needs --sumstats-qc (from `fmg qc`) or --no-sumstats-qc")
    logging.basicConfig(format="%(asctime)s %(name)s %(message)s")
    args.func(args)


if __name__ == "__main__":
    main()
