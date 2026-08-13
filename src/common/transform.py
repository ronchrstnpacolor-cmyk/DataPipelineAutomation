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
        rows.append(
            {
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
            }
        )
    return rows
