# DataPipelineAutomation

Serverless GCP pipeline that ingests Tesla-related news from [NewsAPI](https://newsapi.org/),
stages raw JSON in Cloud Storage, loads it into BigQuery, and curates it for a
Looker Studio dashboard.

Full design, architecture, and step-by-step implementation details: **[DESIGN.md](DESIGN.md)**.

## Quick start

Run from the repo root:

```bash
export PROJECT_ID=<your-gcp-project-id>
export NEWSAPI_API_KEY=<your-newsapi-key>

# 1. Infra
bash infra/create_bucket.sh
bash infra/create_bq_dataset.sh
bash infra/create_secret.sh

# 2. Deploy
bash infra/deploy_hourly_function.sh
bash infra/deploy_daily_function.sh

# 3. Schedule
bash infra/create_scheduler_jobs.sh
bash infra/create_scheduled_query.sh
```

## Local development

```bash
pip install -r src/ingest_hourly/requirements.txt
pip install pytest
PYTHONPATH=src pytest tests/
```

## Layout

- `src/common/` — shared NewsAPI client, transform, GCS/BigQuery/Secret Manager helpers.
- `src/ingest_hourly/` — Diagram 1 Cloud Function (hourly, near-real-time).
- `src/ingest_daily/` — Diagram 2 Cloud Function (daily, archival + curation feed).
- `sql/` — BigQuery raw table schema, curated view, scheduled query.
- `infra/` — `gcloud`/`gsutil`/`bq` deployment scripts.
- `docs/DESIGN.md` — full architecture and implementation guide.
- `docs/README.md` — this file.
