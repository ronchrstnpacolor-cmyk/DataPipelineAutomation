#!/usr/bin/env bash
# Creates the BigQuery dataset, raw table, and curated views.
set -euo pipefail

: "${PROJECT_ID:?Set PROJECT_ID env var}"

ROOT="$(cd "$(dirname "$0")/.." && pwd)"

bq mk --dataset --location=US "${PROJECT_ID}:news_pipeline" || true
bq query --use_legacy_sql=false < "${ROOT}/sql/schema_raw_table.sql"
bq query --use_legacy_sql=false < "${ROOT}/sql/schema_curated_view.sql"

echo "BigQuery dataset, raw table, and curated views are ready."
