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
  targeting_criteria,
  serving_statuses,
  type,
  locale,
  version,
  associated_entity,
  associated_entity_organization_id,
  JSON_EXTRACT_SCALAR(run_schedule, '$.start') as run_schedule_start,
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
  TO_HEX(SHA256(CONCAT(
    COALESCE(CAST(id AS STRING), ''),
    COALESCE(targeting_criteria, ''),
    COALESCE(serving_statuses, ''),
    COALESCE(type, ''),
    COALESCE(locale, ''),
    COALESCE(version, ''),
    COALESCE(associated_entity, ''),
    COALESCE(CAST(associated_entity_organization_id AS STRING), ''),
    COALESCE(run_schedule, ''),
    COALESCE(optimization_target_type, ''),
    COALESCE(change_audit_stamps, ''),
    COALESCE(campaign_group, ''),
    COALESCE(CAST(campaign_group_id AS STRING), ''),
    COALESCE(daily_budget, ''),
    COALESCE(unit_cost, ''),
    COALESCE(creative_selection, ''),
    COALESCE(cost_type, ''),
    COALESCE(name, ''),
    COALESCE(objective_type, ''),
    COALESCE(CAST(offsite_delivery_enabled AS STRING), ''),
    COALESCE(offsite_preferences, ''),
    COALESCE(CAST(audience_expansion_enabled AS STRING), ''),
    COALESCE(CAST(test AS STRING), ''),
    COALESCE(format, ''),
    COALESCE(pacing_strategy, ''),
    COALESCE(account, ''),
    COALESCE(CAST(account_id AS STRING), ''),
    COALESCE(status, ''),
    COALESCE(CAST(created_time AS STRING), ''),
    COALESCE(CAST(last_modified_time AS STRING), ''),
    COALESCE(tenant, ''),
    COALESCE(total_budget, '')
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
      S._gn_start,
      S.id,
      S._gn_active,
      S._gn_end,
      S._gn_synced,
      S.targeting_criteria,
      S.serving_statuses,
      S.type,
      S.locale,
      S.version,
      S.associated_entity,
      S.associated_entity_organization_id,
      S.run_schedule_start,
      S.optimization_target_type,
      S.change_audit_stamps,
      S.campaign_group,
      S.campaign_group_id,
      S.daily_budget,
      S.unit_cost,
      S.creative_selection,
      S.cost_type,
      S.name,
      S.objective_type,
      S.offsite_delivery_enabled,
      S.offsite_preferences,
      S.audience_expansion_enabled,
      S.test,
      S.format,
      S.pacing_strategy,
      S.account,
      S.account_id,
      S.status,
      S.created_time,
      S.last_modified_time,
      S.tenant,
      S.total_budget,
      S._gn_id
    );

COMMIT TRANSACTION;

-- Drop the source table after successful insertion
-- DROP TABLE IF EXISTS `{{source_dataset}}.{{source_table_id}}`;

END IF; 