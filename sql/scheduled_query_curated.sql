-- Scheduled Query: "Refresh Tesla News Curated View"
-- Schedule: Daily, ~30 minutes after the daily ingestion Cloud Scheduler job.
-- Replace ${PROJECT_ID} before running, or pass it via the `bq query` --parameter
-- substitution / a templating step in CI.

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
