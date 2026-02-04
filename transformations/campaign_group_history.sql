-- campaign_group_history
-- SCD Type 2 Transformation for LinkedIn Campaign Group
{% assign target_dataset = vars.target_dataset_id %}
{% assign target_table_id = 'campaign_group_history' %}

{% assign source_dataset = vars.source_dataset_id %}
{% assign source_table_id = 'campaign_groups' %}

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
  id INT64,
  _gn_active BOOL,
  _gn_end TIMESTAMP,
  _gn_synced TIMESTAMP,
  run_schedule_start STRING,
  change_audit_stamps STRING,
  name STRING,
  serving_statuses STRING,
  backfilled BOOL,
  account STRING,
  account_id INT64,
  status STRING,
  test BOOL,
  created_time TIMESTAMP,
  last_modified_time TIMESTAMP,
  tenant STRING,
  total_budget STRING,
  allowed_campaign_types STRING,
  _gn_id STRING
);

/* 
    There are some columns that sometimes don't exist in the source table.
    Add them if they don't exist
*/
ALTER TABLE `{{source_dataset}}.{{source_table_id}}`
    ADD COLUMN IF NOT EXISTS total_budget STRING,
    ADD COLUMN IF NOT EXISTS allowed_campaign_types STRING;

-- Simple merge on id
BEGIN TRANSACTION;

  MERGE `{{target_dataset}}.{{target_table_id}}` T
  USING `{{source_dataset}}.{{source_table_id}}` S
  ON T.id = S.id
    AND T._gn_active = TRUE
  WHEN MATCHED THEN
    UPDATE SET
      _gn_synced = CURRENT_TIMESTAMP(),
      run_schedule_start = CAST(JSON_EXTRACT_SCALAR(S.run_schedule, '$.start') AS STRING),
      change_audit_stamps = CAST(S.change_audit_stamps AS STRING),
      name = CAST(S.name AS STRING),
      serving_statuses = CAST(S.serving_statuses AS STRING),
      backfilled = CAST(S.backfilled AS BOOL),
      account = CAST(S.account AS STRING),
      account_id = CAST(S.account_id AS INT64),
      status = CAST(S.status AS STRING),
      test = CAST(S.test AS BOOL),
      created_time = CAST(S.created_time AS TIMESTAMP),
      last_modified_time = CAST(S.last_modified_time AS TIMESTAMP),
      tenant = CAST(S.tenant AS STRING),
      total_budget = CAST(S.total_budget AS STRING),
      allowed_campaign_types = CAST(S.allowed_campaign_types AS STRING)
  WHEN NOT MATCHED THEN
    INSERT (
      _gn_start,
      id,
      _gn_active,
      _gn_end,
      _gn_synced,
      run_schedule_start,
      change_audit_stamps,
      name,
      serving_statuses,
      backfilled,
      account,
      account_id,
      status,
      test,
      created_time,
      last_modified_time,
      tenant,
      total_budget,
      allowed_campaign_types,
      _gn_id
    )
    VALUES (
      CURRENT_TIMESTAMP(),
      CAST(S.id AS INT64),
      TRUE,
      CAST(NULL AS TIMESTAMP),
      CURRENT_TIMESTAMP(),
      CAST(JSON_EXTRACT_SCALAR(S.run_schedule, '$.start') AS STRING),
      CAST(S.change_audit_stamps AS STRING),
      CAST(S.name AS STRING),
      CAST(S.serving_statuses AS STRING),
      CAST(S.backfilled AS BOOL),
      CAST(S.account AS STRING),
      CAST(S.account_id AS INT64),
      CAST(S.status AS STRING),
      CAST(S.test AS BOOL),
      CAST(S.created_time AS TIMESTAMP),
      CAST(S.last_modified_time AS TIMESTAMP),
      CAST(S.tenant AS STRING),
      CAST(S.total_budget AS STRING),
      CAST(S.allowed_campaign_types AS STRING),
      CAST(NULL AS STRING)
    );

COMMIT TRANSACTION;

END IF; 