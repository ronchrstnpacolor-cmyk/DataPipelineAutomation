import os

PROJECT_ID = os.environ.get("GCP_PROJECT", os.environ.get("GOOGLE_CLOUD_PROJECT"))
BQ_DATASET = os.environ.get("BQ_DATASET", "news_pipeline")
BQ_RAW_TABLE = os.environ.get("BQ_RAW_TABLE", "tesla_news_raw")
GCS_BUCKET = os.environ.get("GCS_BUCKET", f"{PROJECT_ID}-newsapi-raw")
SECRET_NAME = os.environ.get("NEWSAPI_SECRET_NAME", "newsapi-key")
NEWSAPI_QUERY = os.environ.get("NEWSAPI_QUERY", "Tesla")
