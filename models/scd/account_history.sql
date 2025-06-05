-- SCD Type 2 Transformation for LinkedIn Account
{% assign target_dataset = vars.target_dataset_id %}
{% assign target_table_id = 'account_history' %}

{% assign source_dataset = vars.source_dataset_id %}
{% assign source_table_id = 'accounts' %}

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
  change_audit_stamps STRING,
  currency STRING,
  name STRING,
  notified_on_campaign_optimization BOOL,
  notified_on_creative_approval BOOL,
  notified_on_creative_rejection BOOL,
  notified_on_end_of_campaign BOOL,
  notified_on_new_features_enabled BOOL,
  reference STRING,
  reference_organization_id INT64,
  serving_statuses STRING,
  status STRING,
  type STRING,
  test BOOL,
  version STRING,
  created_time TIMESTAMP,
  last_modified_time TIMESTAMP,
  tenant STRING,
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
  change_audit_stamps,
  currency,
  name,
  notified_on_campaign_optimization,
  notified_on_creative_approval,
  notified_on_creative_rejection,
  notified_on_end_of_campaign,
  notified_on_new_features_enabled,
  reference,
  reference_organization_id,
  serving_statuses,
  status,
  type,
  test,
  version,
  created_time,
  last_modified_time,
  tenant,
  TO_HEX(SHA256(CONCAT(
    COALESCE(CAST(id AS STRING), ''),
    COALESCE(change_audit_stamps, ''),
    COALESCE(currency, ''),
    COALESCE(name, ''),
    COALESCE(CAST(notified_on_campaign_optimization AS STRING), ''),
    COALESCE(CAST(notified_on_creative_approval AS STRING), ''),
    COALESCE(CAST(notified_on_creative_rejection AS STRING), ''),
    COALESCE(CAST(notified_on_end_of_campaign AS STRING), ''),
    COALESCE(CAST(notified_on_new_features_enabled AS STRING), ''),
    COALESCE(reference, ''),
    COALESCE(CAST(reference_organization_id AS STRING), ''),
    COALESCE(serving_statuses, ''),
    COALESCE(status, ''),
    COALESCE(type, ''),
    COALESCE(CAST(test AS STRING), ''),
    COALESCE(version, ''),
    COALESCE(CAST(created_time AS STRING), ''),
    COALESCE(CAST(last_modified_time AS STRING), ''),
    COALESCE(tenant, '')
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
      change_audit_stamps,
      currency,
      name,
      notified_on_campaign_optimization,
      notified_on_creative_approval,
      notified_on_creative_rejection,
      notified_on_end_of_campaign,
      notified_on_new_features_enabled,
      reference,
      reference_organization_id,
      serving_statuses,
      status,
      type,
      test,
      version,
      created_time,
      last_modified_time,
      tenant,
      _gn_id
    )
    VALUES (
      S._gn_start,
      S.id,
      S._gn_active,
      S._gn_end,
      S._gn_synced,
      S.change_audit_stamps,
      S.currency,
      S.name,
      S.notified_on_campaign_optimization,
      S.notified_on_creative_approval,
      S.notified_on_creative_rejection,
      S.notified_on_end_of_campaign,
      S.notified_on_new_features_enabled,
      S.reference,
      S.reference_organization_id,
      S.serving_statuses,
      S.status,
      S.type,
      S.test,
      S.version,
      S.created_time,
      S.last_modified_time,
      S.tenant,
      S._gn_id
    );

COMMIT TRANSACTION;

-- Drop the source table after successful insertion
DROP TABLE IF EXISTS `{{source_dataset}}.{{source_table_id}}`;

END IF; 