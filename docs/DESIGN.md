# GCP Data Pipeline Automation — Design & Implementation Guide

## 1. Objective

Design and implement a serverless data pipeline on Google Cloud Platform (GCP), in Python, that ingests Tesla-related news articles from [NewsAPI](https://newsapi.org/), stages the raw data, curates it in BigQuery, and exposes it through a Looker Studio dashboard — optimized for the GCP Free Tier.

**Search criteria:** All articles about Tesla from the last month, sorted by most recent first.
`GET https://newsapi.org/v2/everything?q=Tesla&from=<today-30d>&sortBy=publishedAt&language=en`

---

## 2. Reference Diagrams

- Diagram 1 — Near Real-Time Ingestion Pipeline: [`flow_diagram/Diagram1.png`](flow_diagram/Diagram1.png)
- Diagram 2 — Daily Analytics & Curation Pipeline: [`flow_diagram/Diagram2.png`](flow_diagram/Diagram2.png)

Both pipelines run independently but feed the same BigQuery dataset, so downstream consumers (BigQuery tables/views, Looker Studio) see a unified dataset regardless of which pipeline produced the data.

---

## 3. Architecture Overview

### 3.1 Diagram 1 — Near Real-Time Ingestion (Hourly)

```
Cloud Scheduler (hourly cron)
        │
        ▼
Cloud Function (Python) ──calls──▶ NewsAPI (public REST API)
        │
        ├──▶ Cloud Storage (raw JSON, staging layer)
        │
        └──▶ BigQuery (analytics table, load job)
```

Purpose: keep an analytics-ready table fresh throughout the day with minimal latency.

### 3.2 Diagram 2 — Daily Analytics & Curation (Daily)

```
Cloud Scheduler (daily cron)
        │
        ▼
Secret Manager ──(NewsAPI key)──▶ Cloud Function (Python)
                                        │
                                        ├──▶ Cloud Storage (raw JSON, archive layer)
                                        │
                                        └──▶ BigQuery raw table (partitioned)
                                                    │
                                                    ▼
                                       BigQuery curated view (scheduled query:
                                       dedupe, cleanse, standardize)
                                                    │
                                                    ▼
                                             Looker Studio
                                       (dashboards, KPIs, trends, alerts)
```

Purpose: produce a trustworthy, deduplicated, business-ready dataset once per day for reporting, plus a permanent raw archive for audit/troubleshooting.

### 3.3 Why two pipelines?

| Concern | Diagram 1 (Hourly) | Diagram 2 (Daily) |
|---|---|---|
| Goal | Freshness | Correctness / reporting |
| Secret handling | Optional (can reuse Diagram 2's secret) | Required (Secret Manager) |
| BigQuery target | Analytics table (append) | Raw table (partitioned) → curated view |
| Storage role | Staging | Long-term archive |
| Consumers | Near-real-time dashboards/alerts | Looker Studio, auditing |

Both Cloud Functions ultimately write to the **same GCS bucket** (different prefixes) and the **same BigQuery dataset**, so the raw table in Diagram 2 is the single source of truth; the Diagram 1 analytics table can be a lightweight append-only mirror or, more simply, both functions can write to the same partitioned raw table (recommended for the free tier — see §9).

---

## 4. GCP Services Used

| Service | Purpose | Free tier notes |
|---|---|---|
| Cloud Scheduler | Cron triggers (hourly + daily) | 3 free jobs/month |
| Cloud Functions (2nd gen) | Python ingestion logic | 2M invocations/month free |
| Secret Manager | Store `NEWSAPI_KEY` | 6 active secret versions free |
| Cloud Storage | Raw JSON staging + archive | 5 GB-months free (Standard, US regions) |
| BigQuery | Raw table, curated view, storage | 10 GB storage + 1 TB queries/month free |
| Looker Studio | Dashboards | Free |
| Cloud Logging/Monitoring | Alerts, error tracking | Free tier generous for this volume |

---

## 5. Project Layout

```
DataPipelineAutomation/
├── docs/
│   ├── DESIGN.md                  # this file
│   └── flow_diagram/
├── src/
│   ├── common/
│   │   ├── __init__.py
│   │   ├── newsapi_client.py      # NewsAPI HTTP wrapper
│   │   ├── gcs_writer.py          # Upload raw JSON to GCS
│   │   ├── bq_loader.py           # Load/append to BigQuery
│   │   ├── secrets.py             # Secret Manager access
│   │   ├── transform.py           # Parse/validate/normalize articles
│   │   └── config.py              # Env-driven configuration
│   ├── ingest_hourly/
│   │   ├── main.py                # Cloud Function entry point (Diagram 1)
│   │   └── requirements.txt
│   └── ingest_daily/
│       ├── main.py                # Cloud Function entry point (Diagram 2)
│       └── requirements.txt
├── sql/
│   ├── schema_raw_table.sql
│   ├── schema_curated_view.sql
│   └── scheduled_query_curated.sql
├── infra/
│   ├── deploy_hourly_function.sh
│   ├── deploy_daily_function.sh
│   ├── create_scheduler_jobs.sh
│   ├── create_bucket.sh
│   ├── create_bq_dataset.sh
│   └── create_secret.sh
└── .github/workflows/deploy.yml   # optional CI/CD
```

---

## 6. Python Source Code

### 6.1 `src/common/config.py`

```python
import os

PROJECT_ID = os.environ.get("GCP_PROJECT", os.environ.get("GOOGLE_CLOUD_PROJECT"))
BQ_DATASET = os.environ.get("BQ_DATASET", "news_pipeline")
BQ_RAW_TABLE = os.environ.get("BQ_RAW_TABLE", "tesla_news_raw")
GCS_BUCKET = os.environ.get("GCS_BUCKET", f"{PROJECT_ID}-newsapi-raw")
SECRET_NAME = os.environ.get("NEWSAPI_SECRET_NAME", "newsapi-key")
NEWSAPI_QUERY = os.environ.get("NEWSAPI_QUERY", "Tesla")
```

### 6.2 `src/common/secrets.py`

```python
from google.cloud import secretmanager
from .config import PROJECT_ID, SECRET_NAME


def get_secret(secret_id: str = SECRET_NAME, version: str = "latest") -> str:
    """Fetch a secret value from Google Secret Manager."""
    client = secretmanager.SecretManagerServiceClient()
    name = f"projects/{PROJECT_ID}/secrets/{secret_id}/versions/{version}"
    response = client.access_secret_version(request={"name": name})
    return response.payload.data.decode("UTF-8")
```

### 6.3 `src/common/newsapi_client.py`

```python
import datetime as dt
import requests

NEWSAPI_URL = "https://newsapi.org/v2/everything"


def fetch_tesla_articles(api_key: str, query: str = "Tesla", days_back: int = 30,
                          page_size: int = 100) -> dict:
    """Call NewsAPI for articles matching `query` from the last `days_back` days,
    sorted by most recent first. Paginates until all results are collected."""
    from_date = (dt.datetime.utcnow() - dt.timedelta(days=days_back)).strftime("%Y-%m-%d")

    all_articles = []
    page = 1
    total_results = None

    while True:
        params = {
            "q": query,
            "from": from_date,
            "sortBy": "publishedAt",
            "language": "en",
            "pageSize": page_size,
            "page": page,
            "apiKey": api_key,
        }
        resp = requests.get(NEWSAPI_URL, params=params, timeout=30)
        resp.raise_for_status()
        payload = resp.json()

        if payload.get("status") != "ok":
            raise RuntimeError(f"NewsAPI error: {payload}")

        total_results = payload.get("totalResults", 0)
        all_articles.extend(payload.get("articles", []))

        if len(all_articles) >= total_results or not payload.get("articles"):
            break
        # NewsAPI Developer plan caps at 100 results total — guard against overpaging
        if page * page_size >= 100:
            break
        page += 1

    return {
        "status": "ok",
        "totalResults": total_results,
        "articles": all_articles,
        "fetchedAt": dt.datetime.utcnow().isoformat() + "Z",
        "query": query,
        "fromDate": from_date,
    }
```

### 6.4 `src/common/transform.py`

```python
import hashlib
from typing import Any, Iterable


REQUIRED_FIELDS = ("title", "url", "publishedAt")


def _make_article_id(article: dict) -> str:
    """Stable dedup key: hash of URL (falls back to title+publishedAt)."""
    basis = article.get("url") or f"{article.get('title')}|{article.get('publishedAt')}"
    return hashlib.sha256(basis.encode("utf-8")).hexdigest()


def validate_article(article: dict) -> bool:
    return all(article.get(f) for f in REQUIRED_FIELDS)


def normalize_articles(raw_articles: Iterable[dict], ingestion_ts: str) -> list[dict]:
    """Validate and flatten NewsAPI article records into BigQuery row shape."""
    rows: list[dict[str, Any]] = []
    for a in raw_articles:
        if not validate_article(a):
            continue
        source = a.get("source") or {}
        rows.append({
            "article_id": _make_article_id(a),
            "source_id": source.get("id"),
            "source_name": source.get("name"),
            "author": a.get("author"),
            "title": a.get("title"),
            "description": a.get("description"),
            "url": a.get("url"),
            "url_to_image": a.get("urlToImage"),
            "published_at": a.get("publishedAt"),
            "content": a.get("content"),
            "ingestion_timestamp": ingestion_ts,
        })
    return rows
```

### 6.5 `src/common/gcs_writer.py`

```python
import json
from google.cloud import storage
from .config import GCS_BUCKET


def upload_raw_json(payload: dict, prefix: str, filename: str) -> str:
    """Upload raw NewsAPI response to GCS. Returns the gs:// URI."""
    client = storage.Client()
    bucket = client.bucket(GCS_BUCKET)
    blob_path = f"{prefix}/{filename}"
    blob = bucket.blob(blob_path)
    blob.upload_from_string(
        json.dumps(payload, ensure_ascii=False),
        content_type="application/json",
    )
    return f"gs://{GCS_BUCKET}/{blob_path}"
```

### 6.6 `src/common/bq_loader.py`

```python
from google.cloud import bigquery
from .config import PROJECT_ID, BQ_DATASET, BQ_RAW_TABLE


def load_rows(rows: list[dict]) -> None:
    """Stream-insert normalized article rows into the BigQuery raw table."""
    if not rows:
        return
    client = bigquery.Client(project=PROJECT_ID)
    table_ref = f"{PROJECT_ID}.{BQ_DATASET}.{BQ_RAW_TABLE}"
    errors = client.insert_rows_json(table_ref, rows)
    if errors:
        raise RuntimeError(f"BigQuery insert errors: {errors}")
```

### 6.7 `src/ingest_hourly/main.py` (Diagram 1 — Cloud Function entry point)

```python
import datetime as dt
import functions_framework

from common.config import NEWSAPI_QUERY
from common.secrets import get_secret
from common.newsapi_client import fetch_tesla_articles
from common.gcs_writer import upload_raw_json
from common.transform import normalize_articles
from common.bq_loader import load_rows


@functions_framework.http
def ingest_hourly(request):
    """Triggered hourly by Cloud Scheduler. Fetch, stage, and load Tesla news."""
    api_key = get_secret()  # can also read from env var for lower overhead
    payload = fetch_tesla_articles(api_key, query=NEWSAPI_QUERY, days_back=30)

    ts = dt.datetime.utcnow()
    filename = f"{ts.strftime('%Y%m%dT%H%M%S')}.json"
    gcs_uri = upload_raw_json(payload, prefix="staging/hourly", filename=filename)

    rows = normalize_articles(payload["articles"], ingestion_ts=ts.isoformat() + "Z")
    load_rows(rows)

    return {
        "status": "ok",
        "articlesFetched": len(payload["articles"]),
        "rowsLoaded": len(rows),
        "gcsUri": gcs_uri,
    }, 200
```

### 6.8 `src/ingest_daily/main.py` (Diagram 2 — Cloud Function entry point)

```python
import datetime as dt
import functions_framework

from common.config import NEWSAPI_QUERY
from common.secrets import get_secret
from common.newsapi_client import fetch_tesla_articles
from common.gcs_writer import upload_raw_json
from common.transform import normalize_articles
from common.bq_loader import load_rows


@functions_framework.http
def ingest_daily(request):
    """Triggered daily by Cloud Scheduler. Archives raw JSON and loads the
    partitioned BigQuery raw table that feeds the curated view."""
    api_key = get_secret()  # explicitly required to come from Secret Manager
    payload = fetch_tesla_articles(api_key, query=NEWSAPI_QUERY, days_back=30)

    ts = dt.datetime.utcnow()
    filename = f"{ts.strftime('%Y-%m-%d')}/{ts.strftime('%Y%m%dT%H%M%S')}.json"
    gcs_uri = upload_raw_json(payload, prefix="archive/daily", filename=filename)

    rows = normalize_articles(payload["articles"], ingestion_ts=ts.isoformat() + "Z")
    load_rows(rows)

    return {
        "status": "ok",
        "articlesFetched": len(payload["articles"]),
        "rowsLoaded": len(rows),
        "gcsUri": gcs_uri,
    }, 200
```

### 6.9 `requirements.txt` (both functions)

```
functions-framework==3.*
requests==2.*
google-cloud-storage==2.*
google-cloud-bigquery==3.*
google-cloud-secret-manager==2.*
```

---

## 7. BigQuery Schema Design

### 7.1 Raw table — `news_pipeline.tesla_news_raw`

Partitioned by `published_at` (day granularity), clustered by `source_name` for cheaper filtered scans.

```sql
CREATE TABLE IF NOT EXISTS `news_pipeline.tesla_news_raw` (
  article_id           STRING      NOT NULL OPTIONS(description="SHA-256 of article URL; used for dedup"),
  source_id            STRING,
  source_name          STRING,
  author                STRING,
  title                 STRING      NOT NULL,
  description           STRING,
  url                    STRING      NOT NULL,
  url_to_image          STRING,
  published_at           TIMESTAMP   NOT NULL,
  content                STRING,
  ingestion_timestamp     TIMESTAMP   NOT NULL
)
PARTITION BY DATE(published_at)
CLUSTER BY source_name
OPTIONS(
  description = "Raw, append-only NewsAPI article ingestion table (source of truth for audit).",
  partition_expiration_days = 400
);
```

### 7.2 Curated view — `news_pipeline.tesla_news_curated`

Deduplicated (latest ingestion per `article_id`), cleansed, standardized.

```sql
CREATE OR REPLACE VIEW `news_pipeline.tesla_news_curated` AS
WITH deduped AS (
  SELECT
    *,
    ROW_NUMBER() OVER (
      PARTITION BY article_id
      ORDER BY ingestion_timestamp DESC
    ) AS rn
  FROM `news_pipeline.tesla_news_raw`
)
SELECT
  article_id,
  COALESCE(NULLIF(TRIM(source_name), ''), 'Unknown') AS source_name,
  COALESCE(NULLIF(TRIM(author), ''), 'Unknown')       AS author,
  TRIM(title)                                          AS title,
  TRIM(description)                                    AS description,
  url,
  published_at,
  DATE(published_at)                                   AS published_date,
  LENGTH(content)                                       AS content_length,
  ingestion_timestamp
FROM deduped
WHERE rn = 1
  AND title IS NOT NULL;
```

### 7.3 Optional materialized rollup for dashboard performance

```sql
CREATE OR REPLACE VIEW `news_pipeline.tesla_news_daily_kpis` AS
SELECT
  published_date,
  COUNT(*)                                   AS article_count,
  COUNT(DISTINCT source_name)                AS distinct_sources,
  COUNTIF(author = 'Unknown')                AS unattributed_articles
FROM `news_pipeline.tesla_news_curated`
GROUP BY published_date
ORDER BY published_date DESC;
```

---

## 8. Scheduled Query for the Curated View

BigQuery scheduled queries can't create views directly, but they *can* be used to (a) refresh a materialized snapshot table for performance, or (b) re-run `CREATE OR REPLACE VIEW`. Recommended: run the `CREATE OR REPLACE VIEW` DDL on the same daily cadence right after the daily ingestion Cloud Function completes, guaranteeing the curated layer reflects the latest raw data.

`sql/scheduled_query_curated.sql`:

```sql
-- Scheduled Query: "Refresh Tesla News Curated View"
-- Schedule: Daily, 30 minutes after the daily ingestion Cloud Scheduler job
CREATE OR REPLACE VIEW `${PROJECT_ID}.news_pipeline.tesla_news_curated` AS
WITH deduped AS (
  SELECT
    *,
    ROW_NUMBER() OVER (
      PARTITION BY article_id
      ORDER BY ingestion_timestamp DESC
    ) AS rn
  FROM `${PROJECT_ID}.news_pipeline.tesla_news_raw`
)
SELECT
  article_id,
  COALESCE(NULLIF(TRIM(source_name), ''), 'Unknown') AS source_name,
  COALESCE(NULLIF(TRIM(author), ''), 'Unknown')       AS author,
  TRIM(title)                                          AS title,
  TRIM(description)                                    AS description,
  url,
  published_at,
  DATE(published_at)                                   AS published_date,
  LENGTH(content)                                       AS content_length,
  ingestion_timestamp
FROM deduped
WHERE rn = 1
  AND title IS NOT NULL;
```

Create it via `bq`:

```bash
bq query \
  --use_legacy_sql=false \
  --destination_table="" \
  --schedule="every 24 hours" \
  --display_name="Refresh Tesla News Curated View" \
  --project_id="$PROJECT_ID" \
  < sql/scheduled_query_curated.sql
```

---

## 9. GCP Deployment Configuration

### 9.1 Create the GCS bucket

```bash
# infra/create_bucket.sh
gsutil mb -l US -b on gs://${PROJECT_ID}-newsapi-raw
gsutil lifecycle set infra/gcs_lifecycle.json gs://${PROJECT_ID}-newsapi-raw
```

`infra/gcs_lifecycle.json` (auto-delete staging files after 30 days to stay within the 5 GB free tier; archive/ prefix is kept):

```json
{
  "rule": [
    {
      "action": {"type": "Delete"},
      "condition": {"age": 30, "matchesPrefix": ["staging/"]}
    }
  ]
}
```

### 9.2 Create the BigQuery dataset

```bash
# infra/create_bq_dataset.sh
bq mk --dataset --location=US ${PROJECT_ID}:news_pipeline
bq query --use_legacy_sql=false < sql/schema_raw_table.sql
bq query --use_legacy_sql=false < sql/schema_curated_view.sql
```

### 9.3 Create the secret

```bash
# infra/create_secret.sh
echo -n "$NEWSAPI_API_KEY" | \
  gcloud secrets create newsapi-key --data-file=- --replication-policy="automatic"

# Grant the Cloud Functions service account access
gcloud secrets add-iam-policy-binding newsapi-key \
  --member="serviceAccount:${PROJECT_ID}@appspot.gserviceaccount.com" \
  --role="roles/secretmanager.secretAccessor"
```

### 9.4 Deploy the Cloud Functions (2nd gen)

```bash
# infra/deploy_hourly_function.sh
gcloud functions deploy ingest-hourly \
  --gen2 \
  --runtime=python312 \
  --region=us-central1 \
  --source=src/ingest_hourly \
  --entry-point=ingest_hourly \
  --trigger-http \
  --no-allow-unauthenticated \
  --memory=256Mi \
  --timeout=120s \
  --set-env-vars=GCP_PROJECT=${PROJECT_ID},BQ_DATASET=news_pipeline,BQ_RAW_TABLE=tesla_news_raw,GCS_BUCKET=${PROJECT_ID}-newsapi-raw,NEWSAPI_SECRET_NAME=newsapi-key
```

```bash
# infra/deploy_daily_function.sh
gcloud functions deploy ingest-daily \
  --gen2 \
  --runtime=python312 \
  --region=us-central1 \
  --source=src/ingest_daily \
  --entry-point=ingest_daily \
  --trigger-http \
  --no-allow-unauthenticated \
  --memory=256Mi \
  --timeout=120s \
  --set-env-vars=GCP_PROJECT=${PROJECT_ID},BQ_DATASET=news_pipeline,BQ_RAW_TABLE=tesla_news_raw,GCS_BUCKET=${PROJECT_ID}-newsapi-raw,NEWSAPI_SECRET_NAME=newsapi-key
```

### 9.5 Create Cloud Scheduler jobs

```bash
# infra/create_scheduler_jobs.sh
HOURLY_URL=$(gcloud functions describe ingest-hourly --gen2 --region=us-central1 --format="value(serviceConfig.uri)")
DAILY_URL=$(gcloud functions describe ingest-daily --gen2 --region=us-central1 --format="value(serviceConfig.uri)")

gcloud scheduler jobs create http ingest-hourly-job \
  --schedule="0 * * * *" \
  --uri="$HOURLY_URL" \
  --http-method=POST \
  --oidc-service-account-email="${PROJECT_ID}@appspot.gserviceaccount.com" \
  --location=us-central1

gcloud scheduler jobs create http ingest-daily-job \
  --schedule="0 6 * * *" \
  --uri="$DAILY_URL" \
  --http-method=POST \
  --oidc-service-account-email="${PROJECT_ID}@appspot.gserviceaccount.com" \
  --location=us-central1
```

> **Free-tier simplification:** Cloud Scheduler's free tier covers 3 jobs. Running both an hourly and a daily job uses 2 of those 3, leaving headroom. If cost/quota becomes a concern, the daily pipeline can be dropped in favor of just the hourly pipeline + a daily scheduled query, since both write to the same raw table.

---

## 10. Security Requirements

- **No hardcoded API keys.** `NEWSAPI_API_KEY` is never committed, never set as a Cloud Function env var in plaintext — it is fetched at runtime via `secrets.get_secret()` from Secret Manager (§6.2).
- Cloud Functions run under a dedicated service account with least-privilege IAM roles:
  - `roles/secretmanager.secretAccessor` (scoped to the `newsapi-key` secret)
  - `roles/storage.objectCreator` (scoped to the raw bucket)
  - `roles/bigquery.dataEditor` (scoped to the `news_pipeline` dataset)
- Cloud Functions are deployed with `--no-allow-unauthenticated`; Cloud Scheduler invokes them using OIDC service-account auth.
- `.gitignore` excludes any local `.env` files that might hold a key during local testing.

---

## 11. Looker Studio Dashboard Recommendations

**Data source:** `news_pipeline.tesla_news_curated` (and `tesla_news_daily_kpis` for rollups).

Suggested pages/widgets:
1. **Overview**
   - Scorecards: total articles (last 30 days), articles today, distinct sources, unattributed-author rate.
   - Time series: article volume per day (`published_date` vs. `article_count`).
2. **Source Analysis**
   - Bar chart: top sources by article count.
   - Table: source, article count, latest publish date.
3. **Content Explorer**
   - Table: title, source, author, published_at, url (as link), with a date-range and source filter control.
4. **Trend & Anomaly**
   - Time series with a trailing 7-day moving average to visually flag spikes/drops in Tesla coverage.

**Alerting:** Looker Studio itself doesn't natively alert; combine:
- A Cloud Monitoring alert policy on the daily Cloud Function's error rate / execution failures.
- A scheduled query (or small alerting Cloud Function) that compares today's `article_count` to the 7-day trailing average in `tesla_news_daily_kpis` and emails/Slacks a notification (via Pub/Sub + Cloud Function, or a simple `BigQuery scheduled query` + email report) when the deviation exceeds a threshold (e.g. ±50%).

---

## 12. CI/CD & Monitoring (Optional)

### 12.1 CI/CD — GitHub Actions

`.github/workflows/deploy.yml`:

```yaml
name: Deploy Cloud Functions
on:
  push:
    branches: [main]
    paths:
      - 'src/**'
      - '.github/workflows/deploy.yml'

jobs:
  deploy:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v4
      - uses: google-github-actions/auth@v2
        with:
          workload_identity_provider: ${{ secrets.WIF_PROVIDER }}
          service_account: ${{ secrets.DEPLOY_SA }}
      - uses: google-github-actions/setup-gcloud@v2
      - name: Deploy hourly function
        run: bash infra/deploy_hourly_function.sh
      - name: Deploy daily function
        run: bash infra/deploy_daily_function.sh
```

### 12.2 Monitoring

- Cloud Functions logs flow automatically to Cloud Logging; create log-based metrics for `ERROR` severity.
- Cloud Monitoring alert policy: notify (email/Slack via notification channel) if either function has > 0 executions with status `error` in a 1-hour window.
- BigQuery: monitor `bytes_processed` per scheduled query run to stay within the 1 TB/month free query allowance — the curated view refresh should be cheap since it scans only the raw table, not the whole dataset history (mitigate with partition filters as data grows).

---

## 13. Cost-Efficiency / Free-Tier Checklist

- [x] Cloud Functions 2nd gen, minimal memory (256Mi), short timeout — stays well within 2M free invocations/month (744 hourly + 30 daily ≈ 774/month).
- [x] GCS lifecycle rule deletes staging JSON after 30 days; archive JSON retained (small volume, well under 5 GB).
- [x] BigQuery table partitioned + clustered to minimize bytes scanned per query.
- [x] Only 2 Cloud Scheduler jobs used (of 3 free).
- [x] Secret Manager: 1 secret, well under the 6 free active versions.
- [x] Looker Studio: free regardless of usage.
- [x] No Dataflow/Composer/Pub-Sub required — fully serverless, HTTP-triggered functions keep operational overhead minimal.
