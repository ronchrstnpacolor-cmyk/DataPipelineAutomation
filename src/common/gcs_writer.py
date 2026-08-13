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
