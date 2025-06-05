-- ad_analytics_by_campaign
-- Batch-based daily snapshot table for LinkedIn Ad Analytics with source table existence check
{% assign target_dataset = vars.target_dataset_id %}
{% assign target_table_id = 'ad_analytics_by_campaign' %}

{% assign source_dataset = vars.source_dataset_id %}
{% assign source_table_id = 'ad_analytics_by_campaign' %}

{% assign batch_threshold_seconds = vars.batch_threshold_seconds | default: 120 %}

-- Declare all variables at the top
DECLARE table_exists BOOL DEFAULT FALSE;
DECLARE min_date DATE;
DECLARE max_date DATE;

-- Check if the source table exists
SET table_exists = (
  SELECT COUNT(*) > 0
  FROM `{{source_dataset}}.INFORMATION_SCHEMA.TABLES`
  WHERE table_name = '{{source_table_id}}'
);

-- Only run the ETL logic if the source table exists
IF table_exists THEN

  -- Create target table if it doesn't exist
  CREATE TABLE IF NOT EXISTS `{{target_dataset}}.{{target_table_id}}` (
    campaign_id INT64 NOT NULL,
    date DATE NOT NULL,
    _gn_id STRING,
    campaign STRING,
    clicks INT64,
    impressions INT64,
    cost_in_usd FLOAT64,
    cost_in_local_currency FLOAT64,
    video_views INT64,
    video_completions INT64,
    video_starts INT64,
    total_engagements INT64,
    likes INT64,
    shares INT64,
    comments INT64,
    follows INT64,
    run_id INT64,
    _fivetran_synced TIMESTAMP
  );

  -- Step 1: Create temp table for latest batch
  CREATE TEMP TABLE latest_batch AS
  WITH base AS (
    SELECT * FROM `{{source_dataset}}.{{source_table_id}}`
  ),
  ordered AS (
    SELECT *,
      TIMESTAMP_DIFF(
        _time_extracted,
        LAG(_time_extracted) OVER (ORDER BY _time_extracted),
        SECOND
      ) AS diff_seconds
    FROM base
  ),
  batches AS (
    SELECT *,
      SUM(CASE WHEN diff_seconds IS NULL OR diff_seconds > {{batch_threshold_seconds}} THEN 1 ELSE 0 END)
        OVER (ORDER BY _time_extracted) AS batch_id
    FROM ordered
  ),
  ranked_batches AS (
    SELECT *,
      RANK() OVER (ORDER BY batch_id DESC) AS batch_rank
    FROM batches
  ),
  latest AS (
    SELECT *,
      MAX(UNIX_SECONDS(_time_extracted)) OVER (PARTITION BY batch_id) as run_id
    FROM ranked_batches
    WHERE batch_rank = 1
  )
  SELECT * FROM latest;

  -- Step 2: Assign min/max dates using SET + scalar subqueries
  SET min_date = (
    SELECT MIN(DATE(start_at)) FROM latest_batch
  );

  SET max_date = (
    SELECT MAX(DATE(start_at)) FROM latest_batch
  );

  -- Step 3: Conditional delete and insert
  BEGIN TRANSACTION;

    IF EXISTS (
      SELECT 1
      FROM `{{target_dataset}}.{{target_table_id}}`
      WHERE date BETWEEN min_date AND max_date
        AND campaign_id IN (
          SELECT DISTINCT campaign_id FROM latest_batch
        )
      LIMIT 1
    ) THEN
      DELETE FROM `{{target_dataset}}.{{target_table_id}}`
      WHERE date BETWEEN min_date AND max_date
        AND campaign_id IN (
          SELECT DISTINCT campaign_id FROM latest_batch
        );
    END IF;

    INSERT INTO `{{target_dataset}}.{{target_table_id}}` (
      campaign_id,
      date,
      _gn_id,
      campaign,
      clicks,
      impressions,
      cost_in_usd,
      cost_in_local_currency,
      video_views,
      video_completions,
      video_starts,
      total_engagements,
      likes,
      shares,
      comments,
      follows,
      run_id,
      _fivetran_synced
    )
    SELECT
      campaign_id,
      DATE(start_at) AS date,
      TO_HEX(MD5(TO_JSON_STRING([
        CAST(campaign_id AS STRING),
        CAST(DATE(start_at) AS STRING)
      ]))) AS _gn_id,
      campaign,
      SAFE_CAST(clicks AS INT64),
      SAFE_CAST(impressions AS INT64),
      SAFE_CAST(REGEXP_REPLACE(cost_in_usd, r'[^0-9.]', '') AS FLOAT64),
      SAFE_CAST(REGEXP_REPLACE(cost_in_local_currency, r'[^0-9.]', '') AS FLOAT64),
      SAFE_CAST(video_views AS INT64),
      SAFE_CAST(video_completions AS INT64),
      SAFE_CAST(video_starts AS INT64),
      SAFE_CAST(total_engagements AS INT64),
      SAFE_CAST(likes AS INT64),
      SAFE_CAST(shares AS INT64),
      SAFE_CAST(comments AS INT64),
      SAFE_CAST(follows AS INT64),
      run_id,
      CURRENT_TIMESTAMP() AS _fivetran_synced
    FROM latest_batch;

  COMMIT TRANSACTION;

END IF; 