import datetime as dt

import functions_framework

from common.bq_loader import load_rows
from common.config import NEWSAPI_QUERY
from common.gcs_writer import upload_raw_json
from common.newsapi_client import fetch_tesla_articles
from common.secrets import get_secret
from common.transform import normalize_articles


@functions_framework.http
def ingest_hourly(request):
    """Triggered hourly by Cloud Scheduler (Diagram 1).
    Fetch, stage, and load Tesla news for near-real-time freshness.
    """
    api_key = get_secret()
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
