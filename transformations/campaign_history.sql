-- campaign_history
-- SCD Type 2 Transformation for LinkedIn Campaign
{% assign target_dataset = vars.target_dataset_id %}
{% assign target_table_id = 'campaign_history' %}

{% assign source_dataset = vars.source_dataset_id %}
{% assign source_table_id = 'campaigns' %}

DECLARE table_exists BOOL DEFAULT FALSE;

-- Check if the source table exists
SET table_exists = (
  SELECT COUNT(*) > 0
  FROM `{{source_dataset}}.INFORMATION_SCHEMA.TABLES`
  WHERE table_name = '{{source_table_id}}'
);

-- add missing columns to source

ALTER TABLE `{{source_dataset}}.{{source_table_id}}`
ADD COLUMN IF NOT EXISTS total_budget STRING;

-- Only proceed if the source table exists
IF table_exists THEN

-- Create target table if it doesn't exist
CREATE TABLE IF NOT EXISTS `{{target_dataset}}.{{target_table_id}}` (
  _gn_start TIMESTAMP,
  id INT64,
  _gn_active BOOL,
  _gn_end TIMESTAMP,
  _gn_synced TIMESTAMP,
  targeting_criteria STRING,
  serving_statuses STRING,
  type STRING,
  locale STRING,
  version STRING,
  associated_entity STRING,
  associated_entity_organization_id INT64,
  run_schedule_start STRING,
  optimization_target_type STRING,
  change_audit_stamps STRING,
  campaign_group STRING,
  campaign_group_id INT64,
  daily_budget STRING,
  unit_cost STRING,
  creative_selection STRING,
  cost_type STRING,
  name STRING,
  objective_type STRING,
  offsite_delivery_enabled BOOL,
  offsite_preferences STRING,
  audience_expansion_enabled BOOL,
  test BOOL,
  format STRING,
  pacing_strategy STRING,
  account STRING,
  account_id INT64,
  status STRING,
  created_time TIMESTAMP,
  last_modified_time TIMESTAMP,
  tenant STRING,
  total_budget STRING,
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
      targeting_criteria = CAST(S.targeting_criteria AS STRING),
      serving_statuses = CAST(S.serving_statuses AS STRING),
      type = CAST(S.type AS STRING),
      locale = CAST(S.locale AS STRING),
      version = CAST(S.version AS STRING),
      associated_entity = CAST(S.associated_entity AS STRING),
      associated_entity_organization_id = CAST(S.associated_entity_organization_id AS INT64),
      run_schedule_start = CAST(JSON_EXTRACT_SCALAR(S.run_schedule, '$.start') AS STRING),
      optimization_target_type = CAST(S.optimization_target_type AS STRING),
      change_audit_stamps = CAST(S.change_audit_stamps AS STRING),
      campaign_group = CAST(S.campaign_group AS STRING),
      campaign_group_id = CAST(S.campaign_group_id AS INT64),
      daily_budget = CAST(S.daily_budget AS STRING),
      unit_cost = CAST(S.unit_cost AS STRING),
      creative_selection = CAST(S.creative_selection AS STRING),
      cost_type = CAST(S.cost_type AS STRING),
      name = CAST(S.name AS STRING),
      objective_type = CAST(S.objective_type AS STRING),
      offsite_delivery_enabled = CAST(S.offsite_delivery_enabled AS BOOL),
      offsite_preferences = CAST(S.offsite_preferences AS STRING),
      audience_expansion_enabled = CAST(S.audience_expansion_enabled AS BOOL),
      test = CAST(S.test AS BOOL),
      format = CAST(S.format AS STRING),
      pacing_strategy = CAST(S.pacing_strategy AS STRING),
      account = CAST(S.account AS STRING),
      account_id = CAST(S.account_id AS INT64),
      status = CAST(S.status AS STRING),
      created_time = CAST(S.created_time AS TIMESTAMP),
      last_modified_time = CAST(S.last_modified_time AS TIMESTAMP),
      tenant = CAST(S.tenant AS STRING),
      total_budget = CAST(S.total_budget AS STRING)
  WHEN NOT MATCHED THEN
    INSERT (
      _gn_start,
      id,
      _gn_active,
      _gn_end,
      _gn_synced,
      targeting_criteria,
      serving_statuses,
      type,
      locale,
      version,
      associated_entity,
      associated_entity_organization_id,
      run_schedule_start,
      optimization_target_type,
      change_audit_stamps,
      campaign_group,
      campaign_group_id,
      daily_budget,
      unit_cost,
      creative_selection,
      cost_type,
      name,
      objective_type,
      offsite_delivery_enabled,
      offsite_preferences,
      audience_expansion_enabled,
      test,
      format,
      pacing_strategy,
      account,
      account_id,
      status,
      created_time,
      last_modified_time,
      tenant,
      total_budget,
      _gn_id
    )
    VALUES (
      CURRENT_TIMESTAMP(),
      CAST(S.id AS INT64),
      TRUE,
      CAST(NULL AS TIMESTAMP),
      CURRENT_TIMESTAMP(),
      CAST(S.targeting_criteria AS STRING),
      CAST(S.serving_statuses AS STRING),
      CAST(S.type AS STRING),
      CAST(S.locale AS STRING),
      CAST(S.version AS STRING),
      CAST(S.associated_entity AS STRING),
      CAST(S.associated_entity_organization_id AS INT64),
      CAST(JSON_EXTRACT_SCALAR(S.run_schedule, '$.start') AS STRING),
      CAST(S.optimization_target_type AS STRING),
      CAST(S.change_audit_stamps AS STRING),
      CAST(S.campaign_group AS STRING),
      CAST(S.campaign_group_id AS INT64),
      CAST(S.daily_budget AS STRING),
      CAST(S.unit_cost AS STRING),
      CAST(S.creative_selection AS STRING),
      CAST(S.cost_type AS STRING),
      CAST(S.name AS STRING),
      CAST(S.objective_type AS STRING),
      CAST(S.offsite_delivery_enabled AS BOOL),
      CAST(S.offsite_preferences AS STRING),
      CAST(S.audience_expansion_enabled AS BOOL),
      CAST(S.test AS BOOL),
      CAST(S.format AS STRING),
      CAST(S.pacing_strategy AS STRING),
      CAST(S.account AS STRING),
      CAST(S.account_id AS INT64),
      CAST(S.status AS STRING),
      CAST(S.created_time AS TIMESTAMP),
      CAST(S.last_modified_time AS TIMESTAMP),
      CAST(S.tenant AS STRING),
      CAST(S.total_budget AS STRING),
      CAST(NULL AS STRING)
    );

COMMIT TRANSACTION;

END IF; 