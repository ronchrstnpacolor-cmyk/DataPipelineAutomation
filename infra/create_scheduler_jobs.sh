#!/usr/bin/env bash
# Creates the Cloud Scheduler jobs that trigger the two Cloud Functions.
set -euo pipefail

: "${PROJECT_ID:?Set PROJECT_ID env var}"
REGION="${REGION:-us-central1}"

# Gen2 Cloud Functions run as the default Compute Engine service account
# unless deployed with an explicit --run-service-account, and Cloud Run
# (which backs Gen2 functions) requires the invoker to hold run.invoker.
PROJECT_NUMBER=$(gcloud projects describe "${PROJECT_ID}" --format="value(projectNumber)")
SA_EMAIL="${PROJECT_NUMBER}-compute@developer.gserviceaccount.com"

HOURLY_URL=$(gcloud functions describe ingest-hourly --project="${PROJECT_ID}" --gen2 --region="${REGION}" --format="value(serviceConfig.uri)")
DAILY_URL=$(gcloud functions describe ingest-daily --project="${PROJECT_ID}" --gen2 --region="${REGION}" --format="value(serviceConfig.uri)")

gcloud run services add-iam-policy-binding ingest-hourly \
  --project="${PROJECT_ID}" \
  --region="${REGION}" \
  --member="serviceAccount:${SA_EMAIL}" \
  --role="roles/run.invoker"

gcloud run services add-iam-policy-binding ingest-daily \
  --project="${PROJECT_ID}" \
  --region="${REGION}" \
  --member="serviceAccount:${SA_EMAIL}" \
  --role="roles/run.invoker"

gcloud scheduler jobs create http ingest-hourly-job \
  --project="${PROJECT_ID}" \
  --location="${REGION}" \
  --schedule="0 * * * *" \
  --uri="${HOURLY_URL}" \
  --http-method=POST \
  --oidc-service-account-email="${SA_EMAIL}"

gcloud scheduler jobs create http ingest-daily-job \
  --project="${PROJECT_ID}" \
  --location="${REGION}" \
  --schedule="0 6 * * *" \
  --uri="${DAILY_URL}" \
  --http-method=POST \
  --oidc-service-account-email="${SA_EMAIL}"

echo "Cloud Scheduler jobs created: ingest-hourly-job (hourly), ingest-daily-job (daily 06:00 UTC)."
