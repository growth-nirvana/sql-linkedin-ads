-- account_history
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

-- Simple merge on id
BEGIN TRANSACTION;

  MERGE `{{target_dataset}}.{{target_table_id}}` T
  USING `{{source_dataset}}.{{source_table_id}}` S
  ON T.id = S.id
    AND T._gn_active = TRUE
  WHEN MATCHED THEN
    UPDATE SET
      _gn_synced = CURRENT_TIMESTAMP(),
      change_audit_stamps = CAST(S.change_audit_stamps AS STRING),
      currency = CAST(S.currency AS STRING),
      name = CAST(S.name AS STRING),
      notified_on_campaign_optimization = CAST(S.notified_on_campaign_optimization AS BOOL),
      notified_on_creative_approval = CAST(S.notified_on_creative_approval AS BOOL),
      notified_on_creative_rejection = CAST(S.notified_on_creative_rejection AS BOOL),
      notified_on_end_of_campaign = CAST(S.notified_on_end_of_campaign AS BOOL),
      notified_on_new_features_enabled = CAST(S.notified_on_new_features_enabled AS BOOL),
      reference = CAST(S.reference AS STRING),
      reference_organization_id = CAST(S.reference_organization_id AS INT64),
      serving_statuses = CAST(S.serving_statuses AS STRING),
      status = CAST(S.status AS STRING),
      type = CAST(S.type AS STRING),
      test = CAST(S.test AS BOOL),
      version = CAST(S.version AS STRING),
      created_time = CAST(S.created_time AS TIMESTAMP),
      last_modified_time = CAST(S.last_modified_time AS TIMESTAMP),
      tenant = CAST(S.tenant AS STRING)
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
      CURRENT_TIMESTAMP(),
      CAST(S.id AS INT64),
      TRUE,
      CAST(NULL AS TIMESTAMP),
      CURRENT_TIMESTAMP(),
      CAST(S.change_audit_stamps AS STRING),
      CAST(S.currency AS STRING),
      CAST(S.name AS STRING),
      CAST(S.notified_on_campaign_optimization AS BOOL),
      CAST(S.notified_on_creative_approval AS BOOL),
      CAST(S.notified_on_creative_rejection AS BOOL),
      CAST(S.notified_on_end_of_campaign AS BOOL),
      CAST(S.notified_on_new_features_enabled AS BOOL),
      CAST(S.reference AS STRING),
      CAST(S.reference_organization_id AS INT64),
      CAST(S.serving_statuses AS STRING),
      CAST(S.status AS STRING),
      CAST(S.type AS STRING),
      CAST(S.test AS BOOL),
      CAST(S.version AS STRING),
      CAST(S.created_time AS TIMESTAMP),
      CAST(S.last_modified_time AS TIMESTAMP),
      CAST(S.tenant AS STRING),
      CAST(NULL AS STRING)
    );

COMMIT TRANSACTION;

END IF; 