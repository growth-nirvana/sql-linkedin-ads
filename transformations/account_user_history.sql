-- account_user_history
-- SCD Type 2 Transformation for LinkedIn Account User
{% assign target_dataset = vars.target_dataset_id %}
{% assign target_table_id = 'account_user_history' %}

{% assign source_dataset = vars.source_dataset_id %}
{% assign source_table_id = 'account_users' %}

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
  account_id INT64,
  user_person_id STRING,
  _gn_active BOOL,
  _gn_end TIMESTAMP,
  _gn_synced TIMESTAMP,
  account STRING,
  change_audit_stamps STRING,
  role STRING,
  user STRING,
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
      PARTITION BY account_id, user_person_id 
      ORDER BY _time_extracted DESC
    ) as rn
  FROM `{{source_dataset}}.{{source_table_id}}`
)
SELECT 
  CURRENT_TIMESTAMP() AS _gn_start,
  account_id,
  user_person_id,
  TRUE AS _gn_active,
  CAST(NULL AS TIMESTAMP) AS _gn_end,
  CURRENT_TIMESTAMP() AS _gn_synced,
  account,
  change_audit_stamps,
  role,
  user,
  created_time,
  last_modified_time,
  tenant,
  TO_HEX(SHA256(CONCAT(
    COALESCE(CAST(account_id AS STRING), ''),
    COALESCE(user_person_id, ''),
    COALESCE(account, ''),
    COALESCE(change_audit_stamps, ''),
    COALESCE(role, ''),
    COALESCE(user, ''),
    COALESCE(CAST(created_time AS STRING), ''),
    COALESCE(CAST(last_modified_time AS STRING), ''),
    COALESCE(tenant, '')
  ))) AS _gn_id
FROM base
WHERE rn = 1;

-- Step 2: Handle SCD Type 2 changes
BEGIN TRANSACTION;

  -- Close existing active records that have changed
  UPDATE `{{target_dataset}}.{{target_table_id}}` target
  SET 
    _gn_active = FALSE,
    _gn_end = CURRENT_TIMESTAMP()
  WHERE target._gn_active = TRUE
    AND target.account_id IN (SELECT account_id FROM latest_batch)
    AND target.user_person_id IN (SELECT user_person_id FROM latest_batch)
    AND EXISTS (
      SELECT 1
      FROM latest_batch source
      WHERE source.account_id = target.account_id
        AND source.user_person_id = target.user_person_id
        AND source._gn_id != target._gn_id
    );

  -- Insert new records
  MERGE `{{target_dataset}}.{{target_table_id}}` T
  USING latest_batch S
  ON T.account_id = S.account_id
    AND T.user_person_id = S.user_person_id
    AND T._gn_active = TRUE
  WHEN MATCHED AND T._gn_id != S._gn_id THEN
    UPDATE SET
      _gn_active = FALSE,
      _gn_end = CURRENT_TIMESTAMP()
  WHEN NOT MATCHED THEN
    INSERT (
      _gn_start,
      account_id,
      user_person_id,
      _gn_active,
      _gn_end,
      _gn_synced,
      account,
      change_audit_stamps,
      role,
      user,
      created_time,
      last_modified_time,
      tenant,
      _gn_id
    )
    VALUES (
      S._gn_start,
      S.account_id,
      S.user_person_id,
      S._gn_active,
      S._gn_end,
      S._gn_synced,
      S.account,
      S.change_audit_stamps,
      S.role,
      S.user,
      S.created_time,
      S.last_modified_time,
      S.tenant,
      S._gn_id
    );

COMMIT TRANSACTION;

-- Drop the source table after successful insertion
-- DROP TABLE IF EXISTS `{{source_dataset}}.{{source_table_id}}`;

END IF; 