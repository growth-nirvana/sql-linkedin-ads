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
  run_schedule STRING,
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
  run_schedule,
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
  TO_HEX(SHA256(CONCAT(
    COALESCE(CAST(id AS STRING), ''),
    COALESCE(run_schedule, ''),
    COALESCE(change_audit_stamps, ''),
    COALESCE(name, ''),
    COALESCE(serving_statuses, ''),
    COALESCE(CAST(backfilled AS STRING), ''),
    COALESCE(account, ''),
    COALESCE(CAST(account_id AS STRING), ''),
    COALESCE(status, ''),
    COALESCE(CAST(test AS STRING), ''),
    COALESCE(CAST(created_time AS STRING), ''),
    COALESCE(CAST(last_modified_time AS STRING), ''),
    COALESCE(tenant, ''),
    COALESCE(total_budget, ''),
    COALESCE(allowed_campaign_types, '')
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
      run_schedule,
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
      S._gn_start,
      S.id,
      S._gn_active,
      S._gn_end,
      S._gn_synced,
      S.run_schedule,
      S.change_audit_stamps,
      S.name,
      S.serving_statuses,
      S.backfilled,
      S.account,
      S.account_id,
      S.status,
      S.test,
      S.created_time,
      S.last_modified_time,
      S.tenant,
      S.total_budget,
      S.allowed_campaign_types,
      S._gn_id
    );

COMMIT TRANSACTION;

-- Drop the source table after successful insertion
DROP TABLE IF EXISTS `{{source_dataset}}.{{source_table_id}}`;

END IF; 