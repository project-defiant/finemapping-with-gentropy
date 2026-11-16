#!/usr/bin/env bash
# Download the example study from the GWAS Catalog FTP: GCST90093040, cholesterol in
# chylomicrons and extremely large VLDL (European ancestry, N = 115,082), harmonised to GRCh38.
set -euo pipefail
readonly BASE="https://ftp.ebi.ac.uk/pub/databases/gwas/summary_statistics/GCST90093001-GCST90094000/GCST90093040/harmonised"
readonly FILE="35213538-GCST90093040-EFO_0004574.h.tsv.gz"
mkdir -p data/raw
curl -fL -o "data/raw/${FILE}" "${BASE}/${FILE}"
curl -fL -o "data/raw/${FILE}-meta.yaml" "${BASE}/${FILE}-meta.yaml"
