from google.cloud import bigquery

from .config import BQ_DATASET, BQ_RAW_TABLE, PROJECT_ID


def load_rows(rows: list[dict]) -> None:
    """Stream-insert normalized article rows into the BigQuery raw table."""
    if not rows:
        return
    client = bigquery.Client(project=PROJECT_ID)
    table_ref = f"{PROJECT_ID}.{BQ_DATASET}.{BQ_RAW_TABLE}"
    errors = client.insert_rows_json(table_ref, rows)
    if errors:
        raise RuntimeError(f"BigQuery insert errors: {errors}")
