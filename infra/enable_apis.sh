#!/usr/bin/env bash
# Enables all GCP APIs required by the pipeline.
set -euo pipefail

: "${PROJECT_ID:?Set PROJECT_ID env var}"

gcloud services enable \
  --project="${PROJECT_ID}" \
  cloudfunctions.googleapis.com \
  cloudscheduler.googleapis.com \
  secretmanager.googleapis.com \
  bigquery.googleapis.com \
  storage.googleapis.com \
  cloudbuild.googleapis.com \
  run.googleapis.com \
  artifactregistry.googleapis.com \
  cloudresourcemanager.googleapis.com

echo "Required APIs enabled for ${PROJECT_ID}."
