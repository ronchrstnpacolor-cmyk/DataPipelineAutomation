-- Curated, business-ready view: deduplicated (latest ingestion per article),
-- cleansed, standardized. Backs Looker Studio dashboards (Diagram 2).

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

-- Optional rollup for fast dashboard scorecards/trend charts.
CREATE OR REPLACE VIEW `news_pipeline.tesla_news_daily_kpis` AS
SELECT
  published_date,
  COUNT(*)                    AS article_count,
  COUNT(DISTINCT source_name) AS distinct_sources,
  COUNTIF(author = 'Unknown') AS unattributed_articles
FROM `news_pipeline.tesla_news_curated`
GROUP BY published_date
ORDER BY published_date DESC;
