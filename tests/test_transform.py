import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from common.transform import normalize_articles, validate_article  # noqa: E402

SAMPLE_ARTICLE = {
    "source": {"id": "reuters", "name": "Reuters"},
    "author": "Jane Doe",
    "title": "Tesla unveils new battery tech",
    "description": "A description.",
    "url": "https://example.com/tesla-battery",
    "urlToImage": "https://example.com/img.jpg",
    "publishedAt": "2026-08-10T12:00:00Z",
    "content": "Full content...",
}


def test_validate_article_accepts_complete_article():
    assert validate_article(SAMPLE_ARTICLE) is True


def test_validate_article_rejects_missing_required_field():
    incomplete = dict(SAMPLE_ARTICLE)
    incomplete.pop("title")
    assert validate_article(incomplete) is False


def test_normalize_articles_drops_invalid_and_flattens_valid():
    invalid = dict(SAMPLE_ARTICLE)
    invalid.pop("url")

    rows = normalize_articles([SAMPLE_ARTICLE, invalid], ingestion_ts="2026-08-13T00:00:00Z")

    assert len(rows) == 1
    row = rows[0]
    assert row["source_name"] == "Reuters"
    assert row["title"] == "Tesla unveils new battery tech"
    assert row["ingestion_timestamp"] == "2026-08-13T00:00:00Z"
    assert "article_id" in row and len(row["article_id"]) == 64


def test_normalize_articles_produces_stable_dedup_id_by_url():
    rows_a = normalize_articles([SAMPLE_ARTICLE], ingestion_ts="t1")
    rows_b = normalize_articles([SAMPLE_ARTICLE], ingestion_ts="t2")
    assert rows_a[0]["article_id"] == rows_b[0]["article_id"]
