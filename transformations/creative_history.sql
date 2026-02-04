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

-- Simple merge on id
BEGIN TRANSACTION;

  MERGE `{{target_dataset}}.{{target_table_id}}` T
  USING `{{source_dataset}}.{{source_table_id}}` S
  ON T.id = S.id
    AND T._gn_active = TRUE
  WHEN MATCHED THEN
    UPDATE SET
      _gn_synced = CURRENT_TIMESTAMP(),
      account = CAST(S.account AS STRING),
      account_id = CAST(S.account_id AS INT64),
      campaign = CAST(S.campaign AS STRING),
      campaign_id = CAST(S.campaign_id AS INT64),
      content = CAST(S.content AS STRING),
      created_by = CAST(S.created_by AS STRING),
      last_modified_by = CAST(S.last_modified_by AS STRING),
      intended_status = CAST(S.intended_status AS STRING),
      is_serving = CAST(S.is_serving AS BOOL),
      is_test = CAST(S.is_test AS BOOL),
      created_at = CAST(S.created_at AS TIMESTAMP),
      last_modified_at = CAST(S.last_modified_at AS TIMESTAMP),
      tenant = CAST(S.tenant AS STRING),
      serving_hold_reasons = CAST(S.serving_hold_reasons AS STRING)
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
      CURRENT_TIMESTAMP(),
      CAST(S.id AS STRING),
      TRUE,
      CAST(NULL AS TIMESTAMP),
      CURRENT_TIMESTAMP(),
      CAST(S.account AS STRING),
      CAST(S.account_id AS INT64),
      CAST(S.campaign AS STRING),
      CAST(S.campaign_id AS INT64),
      CAST(S.content AS STRING),
      CAST(S.created_by AS STRING),
      CAST(S.last_modified_by AS STRING),
      CAST(S.intended_status AS STRING),
      CAST(S.is_serving AS BOOL),
      CAST(S.is_test AS BOOL),
      CAST(S.created_at AS TIMESTAMP),
      CAST(S.last_modified_at AS TIMESTAMP),
      CAST(S.tenant AS STRING),
      CAST(S.serving_hold_reasons AS STRING),
      CAST(NULL AS STRING)
    );

COMMIT TRANSACTION;

END IF; 