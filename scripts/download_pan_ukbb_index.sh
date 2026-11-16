#!/usr/bin/env bash
# Copy the Pan-UKBB GRCh38 variant index (about 1.3 GB, 56,762 files) from the public AWS
# bucket. No AWS account is needed. Reading it from S3 through Hail takes hours, because the
# files are read one at a time; a parallel sync takes minutes.
#
# Usage: scripts/download_pan_ukbb_index.sh [EUR|CSA|AFR]
set -euo pipefail
readonly POP="${1:-EUR}"
readonly SRC="s3://pan-ukb-us-east-1/ld_release/UKBB.${POP}.ldadj.variant.b38.ht"
readonly DST="data/pan_ukbb/UKBB.${POP}.ldadj.variant.b38.ht"

# A throwaway AWS config, so the user's own is left alone.
config="$(mktemp)"
trap 'rm -f "${config}"' EXIT
printf '[default]\ns3 =\n    max_concurrent_requests = 64\n' > "${config}"

mkdir -p "${DST}"
AWS_CONFIG_FILE="${config}" aws s3 sync --no-sign-request --only-show-errors "${SRC}" "${DST}"
