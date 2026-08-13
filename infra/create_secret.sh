#!/usr/bin/env bash
# Creates the newsapi-key secret in Secret Manager and grants the Cloud
# Functions runtime service account access to it.
#
# Usage: NEWSAPI_API_KEY=xxxx PROJECT_ID=my-project ./create_secret.sh
set -euo pipefail

: "${PROJECT_ID:?Set PROJECT_ID env var}"
: "${NEWSAPI_API_KEY:?Set NEWSAPI_API_KEY env var}"

echo -n "${NEWSAPI_API_KEY}" | \
  gcloud secrets create newsapi-key \
    --project="${PROJECT_ID}" \
    --data-file=- \
    --replication-policy="automatic"

gcloud secrets add-iam-policy-binding newsapi-key \
  --project="${PROJECT_ID}" \
  --member="serviceAccount:${PROJECT_ID}@appspot.gserviceaccount.com" \
  --role="roles/secretmanager.secretAccessor"

echo "Secret 'newsapi-key' created and access granted."
