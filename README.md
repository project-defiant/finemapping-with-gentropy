# finemapping-with-gentropy

Fine-map one GWAS with [gentropy](https://github.com/opentargets/gentropy) the way Open Targets
does: locus-breaker clumping, SuSiE-inf per locus and credible set QC, with the production
parameters. Everything runs on a laptop from public data. No Open Targets bucket and no cloud
account are needed.

## Requirements

- Java 17 (Spark and Hail)
- [uv](https://docs.astral.sh/uv/)
- The AWS CLI, for the one-off copy of the Pan-UKBB variant index (no account needed)
- About 12 GB of memory free for Spark, and network access to AWS `us-east-1`

```bash
uv sync   # Python 3.11, gentropy 3.4.0, ruff
```

## Run the example

The example is GCST90093040, cholesterol in chylomicrons and extremely large VLDL: European
ancestry, N = 115,082, GRCh38.

```bash
# 1. Summary statistics from the GWAS Catalog FTP (GWAS-SSF, harmonised)
scripts/download_example.sh

# 2. Convert to gentropy SummaryStatistics (parse + sanity_filter, as Open Targets does)
uv run fmg ingest --input data/raw/35213538-GCST90093040-EFO_0004574.h.tsv.gz \
  --output data/sumstats/GCST90093040

# 3. Once per LD population: copy the Pan-UKBB GRCh38 variant index (1.3 GB)
scripts/download_pan_ukbb_index.sh EUR

# 4. Once per LD population: align its alleles to the summary statistics
uv run fmg ld-index --sumstats data/sumstats/GCST90093040 --output data/ld_index \
  --variant-table "data/pan_ukbb/UKBB.{POP}.ldadj.variant.b38.ht"

# 5. Study index -> locus breaker -> SuSiE-inf -> credible set QC
uv run fmg run --sumstats data/sumstats/GCST90093040 --study-id GCST90093040 \
  --n-samples 115082 --ld-population nfe \
  --ld-source public --ld-index data/ld_index --out out/GCST90093040
```

`fmg run` options:

- `--regions 19:44500000-45300000 ...` fine-maps only loci whose lead is in a region.
- `--max-loci N` fine-maps the first N loci.
- `--skip-clumping` reuses the clumped loci.
- `--qc-only` reruns credible set QC alone.
- `--ld-source opentargets` reads the Open Targets copy of the LD (Open Targets credentials).

## Your own study

1. Format the summary statistics as [GWAS-SSF](https://github.com/EBISPOT/gwas-summary-statistics-standard) on GRCh38. A file harmonised by the GWAS Catalog already is.
2. Pass `--study-id` to `fmg ingest` unless the file name has a `GCST` id.
3. Give `fmg run` the study's sample size (`--n-samples`) and LD population (`--ld-population`): `nfe` for European, `afr` for African, `csa` for Central and South Asian. These are the Pan-UKBB populations.

`fmg run` writes the study index row the fine-mapper needs (`pipeline.write_study_index`).
The fine-mapper skips the study unless that row passes every gentropy check, so the row is
written to pass them.

## Outputs (`out/<study>/`)

| Path | Content |
| --- | --- |
| `study_locus_lb_clumped/` | Loci from the locus breaker, one partition per `studyLocusId` |
| `credible_sets/<locus>/` | SuSiE-inf credible sets of each locus (`StudyLocus`, variants in `locus`) |
| `logs/<locus>.log` | Per-locus counts, run time, number of credible sets, or the reason it was skipped |
| `credible_sets_clean/` | All credible sets after QC: lead p ≤ 1e-5, purity min r² ≥ 0.25, one per study, lead and method |

## Where the parameters come from

`src/finemapping_with_gentropy/config.py` copies the Open Targets production configuration in
[opentargets/pipeline](https://github.com/opentargets/pipeline) (`orchestration`):

- `gwas_catalog_sumstats_susie_clumping.yaml`
- `assets/susie_finemap.sh`
- `credible_set_qc.yaml`

## Notes

- **Pan-UKBB index.** Open Targets keeps an allele-aligned Pan-UKBB index on a private bucket. `fmg ld-index` rebuilds the equivalent from the public release, aligning the alleles to the summary statistics instead of the Open Targets variant annotation.
- **Reading LD from S3.** Only the LD block matrix is read from S3 during fine-mapping, for each locus's variants. Reading the variant index from S3 through Hail instead of a local copy takes hours.
- **gentropy version.** This repo pins gentropy 3.4.0. gentropy `dev`, after #1326, cannot read the locus breaker output in the fine-mapper and no longer applies the study checks in SuSiE-inf.
