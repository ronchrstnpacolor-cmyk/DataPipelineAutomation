#!/usr/bin/env bash
# Creates the BigQuery scheduled query that refreshes the curated view daily,
# ~30 minutes after the daily ingestion Cloud Scheduler job runs.
set -euo pipefail

: "${PROJECT_ID:?Set PROJECT_ID env var}"

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
QUERY_FILE="${ROOT}/sql/scheduled_query_curated.sql"

# Substitute ${PROJECT_ID} into the query, and strip leading "--" comment
# lines: bq's flag parser treats a positional argument starting with "--"
# as an (unrecognized) flag, which crashes its typo-suggestion code.
RENDERED=$(sed "s/\${PROJECT_ID}/${PROJECT_ID}/g" "${QUERY_FILE}" | grep -v '^--')

bq query \
  --project_id="${PROJECT_ID}" \
  --use_legacy_sql=false \
  --schedule="every 24 hours" \
  --display_name="Refresh Tesla News Curated View" \
  --target_dataset="news_pipeline" \
  --location="US" \
  --nouse_cache \
  -- \
  "${RENDERED}"

echo "Scheduled query 'Refresh Tesla News Curated View' created."
