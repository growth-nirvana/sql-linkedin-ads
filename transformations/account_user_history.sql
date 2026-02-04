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

-- Simple merge on account_id and user_person_id
BEGIN TRANSACTION;

  MERGE `{{target_dataset}}.{{target_table_id}}` T
  USING `{{source_dataset}}.{{source_table_id}}` S
  ON T.account_id = S.account_id
    AND T.user_person_id = S.user_person_id
    AND T._gn_active = TRUE
  WHEN MATCHED THEN
    UPDATE SET
      _gn_synced = CURRENT_TIMESTAMP(),
      account = CAST(S.account AS STRING),
      change_audit_stamps = CAST(S.change_audit_stamps AS STRING),
      role = CAST(S.role AS STRING),
      user = CAST(S.user AS STRING),
      created_time = CAST(S.created_time AS TIMESTAMP),
      last_modified_time = CAST(S.last_modified_time AS TIMESTAMP),
      tenant = CAST(S.tenant AS STRING)
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
      CURRENT_TIMESTAMP(),
      CAST(S.account_id AS INT64),
      CAST(S.user_person_id AS STRING),
      TRUE,
      CAST(NULL AS TIMESTAMP),
      CURRENT_TIMESTAMP(),
      CAST(S.account AS STRING),
      CAST(S.change_audit_stamps AS STRING),
      CAST(S.role AS STRING),
      CAST(S.user AS STRING),
      CAST(S.created_time AS TIMESTAMP),
      CAST(S.last_modified_time AS TIMESTAMP),
      CAST(S.tenant AS STRING),
      CAST(NULL AS STRING)
    );

COMMIT TRANSACTION;

END IF; 