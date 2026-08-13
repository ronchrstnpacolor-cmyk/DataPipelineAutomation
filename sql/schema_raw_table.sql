-- Raw, append-only NewsAPI article ingestion table.
-- Partitioned by publish date, clustered by source for cheap filtered scans.
-- Source of truth for audit/troubleshooting (Diagram 2).

CREATE TABLE IF NOT EXISTS `news_pipeline.tesla_news_raw` (
  article_id            STRING    NOT NULL OPTIONS(description="SHA-256 of article URL; used for dedup"),
  source_id              STRING,
  source_name            STRING,
  author                  STRING,
  title                   STRING    NOT NULL,
  description             STRING,
  url                      STRING    NOT NULL,
  url_to_image            STRING,
  published_at             TIMESTAMP NOT NULL,
  content                  STRING,
  ingestion_timestamp       TIMESTAMP NOT NULL
)
PARTITION BY DATE(published_at)
CLUSTER BY source_name
OPTIONS(
  description = "Raw, append-only NewsAPI article ingestion table (source of truth for audit).",
  partition_expiration_days = 400
);
