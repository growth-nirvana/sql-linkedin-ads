-- creative_history
-- SCD Type 2 Transformation for LinkedIn Creative
{% assign target_dataset = vars.target_dataset_id %}
{% assign target_table_id = 'creative_history' %}

{% assign source_dataset = vars.source_dataset_id %}
{% assign source_table_id = 'creatives' %}

DECLARE table_exists BOOL DEFAULT FALSE;

-- Check if the source table exists
SET table_exists = (
  SELECT COUNT(*) > 0
  FROM `{{source_dataset}}.INFORMATION_SCHEMA.TABLES`
  WHERE table_name = '{{source_table_id}}'
);

-- Only proceed if the source table exists
IF table_exists THEN

-- Create target table if it doesn't exist
CREATE TABLE IF NOT EXISTS `{{target_dataset}}.{{target_table_id}}` (
  _gn_start TIMESTAMP,
  id STRING,
  _gn_active BOOL,
  _gn_end TIMESTAMP,
  _gn_synced TIMESTAMP,
  account STRING,
  account_id INT64,
  campaign STRING,
  campaign_id INT64,
  content STRING,
  created_by STRING,
  last_modified_by STRING,
  intended_status STRING,
  is_serving BOOL,
  is_test BOOL,
  created_at TIMESTAMP,
  last_modified_at TIMESTAMP,
  tenant STRING,
  serving_hold_reasons STRING,
  _gn_id STRING
);

-- Step 1: Create temp table for latest batch with deduplication
CREATE TEMP TABLE latest_batch AS
WITH base AS (
  SELECT *,
    ROW_NUMBER() OVER (
      PARTITION BY id 
      ORDER BY _time_extracted DESC
    ) as rn
  FROM `{{source_dataset}}.{{source_table_id}}`
)
SELECT 
  CURRENT_TIMESTAMP() AS _gn_start,
  id,
  TRUE AS _gn_active,
  CAST(NULL AS TIMESTAMP) AS _gn_end,
  CURRENT_TIMESTAMP() AS _gn_synced,
  account,
  account_id,
  campaign,
  campaign_id,
  content,
  created_by,
  last_modified_by,
  intended_status,
  is_serving,
  is_test,
  created_at,
  last_modified_at,
  tenant,
  serving_hold_reasons,
  TO_HEX(SHA256(CONCAT(
    COALESCE(id, ''),
    COALESCE(account, ''),
    COALESCE(CAST(account_id AS STRING), ''),
    COALESCE(campaign, ''),
    COALESCE(CAST(campaign_id AS STRING), ''),
    COALESCE(content, ''),
    COALESCE(created_by, ''),
    COALESCE(last_modified_by, ''),
    COALESCE(intended_status, ''),
    COALESCE(CAST(is_serving AS STRING), ''),
    COALESCE(CAST(is_test AS STRING), ''),
    COALESCE(CAST(created_at AS STRING), ''),
    COALESCE(CAST(last_modified_at AS STRING), ''),
    COALESCE(tenant, ''),
    COALESCE(serving_hold_reasons, '')
  ))) AS _gn_id
FROM base
WHERE rn = 1;

-- Step 2: Handle SCD Type 2 changes
BEGIN TRANSACTION;

  MERGE `{{target_dataset}}.{{target_table_id}}` T
  USING latest_batch S
  ON T.id = S.id
    AND T._gn_active = TRUE
  WHEN MATCHED AND T._gn_id != S._gn_id THEN
    UPDATE SET
      _gn_active = FALSE,
      _gn_end = CURRENT_TIMESTAMP()
  WHEN NOT MATCHED THEN
    INSERT (
      _gn_start,
      id,
      _gn_active,
      _gn_end,
      _gn_synced,
      account,
      account_id,
      campaign,
      campaign_id,
      content,
      created_by,
      last_modified_by,
      intended_status,
      is_serving,
      is_test,
      created_at,
      last_modified_at,
      tenant,
      serving_hold_reasons,
      _gn_id
    )
    VALUES (
      S._gn_start,
      S.id,
      S._gn_active,
      S._gn_end,
      S._gn_synced,
      S.account,
      S.account_id,
      S.campaign,
      S.campaign_id,
      S.content,
      S.created_by,
      S.last_modified_by,
      S.intended_status,
      S.is_serving,
      S.is_test,
      S.created_at,
      S.last_modified_at,
      S.tenant,
      S.serving_hold_reasons,
      S._gn_id
    );

COMMIT TRANSACTION;

-- Drop the source table after successful insertion
-- DROP TABLE IF EXISTS `{{source_dataset}}.{{source_table_id}}`;

END IF; 