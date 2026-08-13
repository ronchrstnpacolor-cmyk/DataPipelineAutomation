#!/usr/bin/env bash
# Deploys the hourly ingestion Cloud Function (Diagram 1).
#
# Cloud Functions only uploads the specified --source directory, so the
# shared `common` package is copied alongside main.py before deploying and
# removed again afterward.
set -euo pipefail

: "${PROJECT_ID:?Set PROJECT_ID env var}"
REGION="${REGION:-us-central1}"

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
SRC_DIR="${ROOT}/src/ingest_hourly"
COMMON_DIR="${ROOT}/src/common"

cleanup() { rm -rf "${SRC_DIR}/common"; }
trap cleanup EXIT

cp -r "${COMMON_DIR}" "${SRC_DIR}/common"

gcloud functions deploy ingest-hourly \
  --project="${PROJECT_ID}" \
  --gen2 \
  --runtime=python312 \
  --region="${REGION}" \
  --source="${SRC_DIR}" \
  --entry-point=ingest_hourly \
  --trigger-http \
  --no-allow-unauthenticated \
  --memory=256Mi \
  --timeout=120s \
  --set-env-vars="GCP_PROJECT=${PROJECT_ID},BQ_DATASET=news_pipeline,BQ_RAW_TABLE=tesla_news_raw,GCS_BUCKET=${PROJECT_ID}-newsapi-raw,NEWSAPI_SECRET_NAME=newsapi-key"
