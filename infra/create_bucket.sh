#!/usr/bin/env bash
# Creates the GCS bucket used for raw JSON staging (hourly) and archiving (daily).
set -euo pipefail

: "${PROJECT_ID:?Set PROJECT_ID env var}"

BUCKET="gs://${PROJECT_ID}-newsapi-raw"

gsutil mb -l US -b on "${BUCKET}"
gsutil lifecycle set "$(dirname "$0")/gcs_lifecycle.json" "${BUCKET}"

echo "Created and configured ${BUCKET}"
