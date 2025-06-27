-- ad_analytics_by_creative
-- Batch-based daily snapshot table for LinkedIn Ad Analytics by Creative with source table existence check
{% assign target_dataset = vars.target_dataset_id %}
{% assign target_table_id = 'ad_analytics_by_creative' %}

{% assign source_dataset = vars.source_dataset_id %}
{% assign source_table_id = 'ad_analytics_by_creative' %}

{% assign batch_threshold_seconds = vars.batch_threshold_seconds | default: 120 %}

-- Declare all variables at the top
DECLARE table_exists BOOL DEFAULT FALSE;
DECLARE min_date DATE;
DECLARE max_date DATE;

-- Check if the source table exists
SET table_exists = (
  SELECT COUNT(*) > 0
  FROM `{{source_dataset}}.INFORMATION_SCHEMA.TABLES`
  WHERE table_name = '{{source_table_id}}'
);

-- Only run the ETL logic if the source table exists
IF table_exists THEN

-- add missing columns to source
-- (No ALTER TABLE needed for derived or NOT NULL columns)
ALTER TABLE `{{source_dataset}}.{{source_table_id}}`
  ADD COLUMN IF NOT EXISTS landing_page_clicks INT64,
  ADD COLUMN IF NOT EXISTS document_completions INT64,
  ADD COLUMN IF NOT EXISTS document_first_quartile_completions INT64,
  ADD COLUMN IF NOT EXISTS document_midpoint_completions INT64,
  ADD COLUMN IF NOT EXISTS document_third_quartile_completions INT64,
  ADD COLUMN IF NOT EXISTS download_clicks INT64,
  ADD COLUMN IF NOT EXISTS impressions INT64,
  ADD COLUMN IF NOT EXISTS viral_document_completions INT64,
  ADD COLUMN IF NOT EXISTS viral_document_first_quartile_completions INT64,
  ADD COLUMN IF NOT EXISTS full_screen_plays INT64,
  ADD COLUMN IF NOT EXISTS viral_document_midpoint_completions INT64,
  ADD COLUMN IF NOT EXISTS viral_document_third_quartile_completions INT64,
  ADD COLUMN IF NOT EXISTS viral_download_clicks INT64,
  ADD COLUMN IF NOT EXISTS video_completions INT64,
  ADD COLUMN IF NOT EXISTS card_clicks INT64,
  ADD COLUMN IF NOT EXISTS card_impressions INT64,
  ADD COLUMN IF NOT EXISTS comment_likes INT64,
  ADD COLUMN IF NOT EXISTS viral_card_clicks INT64,
  ADD COLUMN IF NOT EXISTS viral_card_impressions INT64,
  ADD COLUMN IF NOT EXISTS viral_comment_likes INT64,
  ADD COLUMN IF NOT EXISTS action_clicks INT64,
  ADD COLUMN IF NOT EXISTS ad_unit_clicks INT64,
  ADD COLUMN IF NOT EXISTS clicks INT64,
  ADD COLUMN IF NOT EXISTS comments INT64,
  ADD COLUMN IF NOT EXISTS company_page_clicks INT64,
  ADD COLUMN IF NOT EXISTS conversion_value_in_local_currency STRING,
  ADD COLUMN IF NOT EXISTS cost_in_local_currency STRING,
  ADD COLUMN IF NOT EXISTS cost_in_usd STRING,
  ADD COLUMN IF NOT EXISTS date_range STRING,
  ADD COLUMN IF NOT EXISTS external_website_conversions INT64,
  ADD COLUMN IF NOT EXISTS external_website_post_click_conversions INT64,
  ADD COLUMN IF NOT EXISTS external_website_post_view_conversions INT64,
  ADD COLUMN IF NOT EXISTS follows INT64,
  ADD COLUMN IF NOT EXISTS lead_generation_mail_contact_info_shares INT64,
  ADD COLUMN IF NOT EXISTS lead_generation_mail_interested_clicks INT64,
  ADD COLUMN IF NOT EXISTS likes INT64,
  ADD COLUMN IF NOT EXISTS one_click_lead_form_opens INT64,
  ADD COLUMN IF NOT EXISTS one_click_leads INT64,
  ADD COLUMN IF NOT EXISTS opens INT64,
  ADD COLUMN IF NOT EXISTS other_engagements INT64,
  ADD COLUMN IF NOT EXISTS pivot STRING,
  ADD COLUMN IF NOT EXISTS pivot_value STRING,
  ADD COLUMN IF NOT EXISTS pivot_values STRING,
  ADD COLUMN IF NOT EXISTS reactions INT64,
  ADD COLUMN IF NOT EXISTS sends INT64,
  ADD COLUMN IF NOT EXISTS shares INT64,
  ADD COLUMN IF NOT EXISTS text_url_clicks INT64,
  ADD COLUMN IF NOT EXISTS total_engagements INT64,
  ADD COLUMN IF NOT EXISTS video_first_quartile_completions INT64,
  ADD COLUMN IF NOT EXISTS video_midpoint_completions INT64,
  ADD COLUMN IF NOT EXISTS video_starts INT64,
  ADD COLUMN IF NOT EXISTS video_third_quartile_completions INT64,
  ADD COLUMN IF NOT EXISTS video_views INT64,
  ADD COLUMN IF NOT EXISTS viral_clicks INT64,
  ADD COLUMN IF NOT EXISTS viral_comments INT64,
  ADD COLUMN IF NOT EXISTS viral_company_page_clicks INT64,
  ADD COLUMN IF NOT EXISTS viral_follows INT64,
  ADD COLUMN IF NOT EXISTS viral_full_screen_plays INT64,
  ADD COLUMN IF NOT EXISTS viral_impressions INT64,
  ADD COLUMN IF NOT EXISTS viral_landing_page_clicks INT64,
  ADD COLUMN IF NOT EXISTS viral_likes INT64,
  ADD COLUMN IF NOT EXISTS viral_one_click_lead_form_opens INT64,
  ADD COLUMN IF NOT EXISTS viral_one_click_leads INT64,
  ADD COLUMN IF NOT EXISTS viral_other_engagements INT64,
  ADD COLUMN IF NOT EXISTS viral_reactions INT64,
  ADD COLUMN IF NOT EXISTS viral_shares INT64,
  ADD COLUMN IF NOT EXISTS viral_total_engagements INT64,
  ADD COLUMN IF NOT EXISTS viral_video_completions INT64,
  ADD COLUMN IF NOT EXISTS viral_video_first_quartile_completions INT64,
  ADD COLUMN IF NOT EXISTS approximate_member_reach INT64,
  ADD COLUMN IF NOT EXISTS viral_external_website_conversions INT64,
  ADD COLUMN IF NOT EXISTS viral_external_website_post_click_conversions INT64,
  ADD COLUMN IF NOT EXISTS viral_external_website_post_view_conversions INT64,
  ADD COLUMN IF NOT EXISTS viral_video_midpoint_completions INT64,
  ADD COLUMN IF NOT EXISTS viral_video_starts INT64,
  ADD COLUMN IF NOT EXISTS viral_video_third_quartile_completions INT64,
  ADD COLUMN IF NOT EXISTS viral_video_views INT64;


  -- Create target table if it doesn't exist
  CREATE TABLE IF NOT EXISTS `{{target_dataset}}.{{target_table_id}}` (
    creative_id INT64 NOT NULL,
    day DATE NOT NULL,
    _gn_id STRING,
    creative STRING,
    landing_page_clicks INT64,
    document_completions INT64,
    document_first_quartile_completions INT64,
    document_midpoint_completions INT64,
    document_third_quartile_completions INT64,
    download_clicks INT64,
    impressions INT64,
    viral_document_completions INT64,
    viral_document_first_quartile_completions INT64,
    full_screen_plays INT64,
    viral_document_midpoint_completions INT64,
    viral_document_third_quartile_completions INT64,
    viral_download_clicks INT64,
    video_completions INT64,
    card_clicks INT64,
    card_impressions INT64,
    comment_likes INT64,
    viral_card_clicks INT64,
    viral_card_impressions INT64,
    viral_comment_likes INT64,
    action_clicks INT64,
    ad_unit_clicks INT64,
    clicks INT64,
    comments INT64,
    company_page_clicks INT64,
    conversion_value_in_local_currency FLOAT64,
    cost_in_local_currency FLOAT64,
    cost_in_usd FLOAT64,
    date_range STRING,
    external_website_conversions INT64,
    external_website_post_click_conversions INT64,
    external_website_post_view_conversions INT64,
    follows INT64,
    lead_generation_mail_contact_info_shares INT64,
    lead_generation_mail_interested_clicks INT64,
    likes INT64,
    one_click_lead_form_opens INT64,
    one_click_leads INT64,
    opens INT64,
    other_engagements INT64,
    pivot STRING,
    pivot_value STRING,
    pivot_values STRING,
    reactions INT64,
    sends INT64,
    shares INT64,
    text_url_clicks INT64,
    total_engagements INT64,
    video_first_quartile_completions INT64,
    video_midpoint_completions INT64,
    video_starts INT64,
    video_third_quartile_completions INT64,
    video_views INT64,
    viral_clicks INT64,
    viral_comments INT64,
    viral_company_page_clicks INT64,
    viral_follows INT64,
    viral_full_screen_plays INT64,
    viral_impressions INT64,
    viral_landing_page_clicks INT64,
    viral_likes INT64,
    viral_one_click_lead_form_opens INT64,
    viral_one_click_leads INT64,
    viral_other_engagements INT64,
    viral_reactions INT64,
    viral_shares INT64,
    viral_total_engagements INT64,
    viral_video_completions INT64,
    viral_video_first_quartile_completions INT64,
    viral_video_midpoint_completions INT64,
    viral_video_starts INT64,
    viral_video_third_quartile_completions INT64,
    viral_video_views INT64,
    approximate_member_reach INT64,
    tenant STRING,
    run_id INT64,
    _fivetran_synced TIMESTAMP,
    start_at TIMESTAMP,
    end_at TIMESTAMP,
    _time_extracted TIMESTAMP,
    _time_loaded TIMESTAMP,
    viral_external_website_conversions INT64,
    viral_external_website_post_click_conversions INT64,
    viral_external_website_post_view_conversions INT64
  );

  -- Step 1: Create temp table for latest batch
  CREATE TEMP TABLE latest_batch AS
  WITH base AS (
    SELECT * FROM `{{source_dataset}}.{{source_table_id}}`
  ),
  ordered AS (
    SELECT *,
      TIMESTAMP_DIFF(
        _time_extracted,
        LAG(_time_extracted) OVER (ORDER BY _time_extracted),
        SECOND
      ) AS diff_seconds
    FROM base
  ),
  batches AS (
    SELECT *,
      SUM(CASE WHEN diff_seconds IS NULL OR diff_seconds > {{batch_threshold_seconds}} THEN 1 ELSE 0 END)
        OVER (ORDER BY _time_extracted) AS batch_id
    FROM ordered
  ),
  ranked_batches AS (
    SELECT *,
      RANK() OVER (ORDER BY batch_id DESC) AS batch_rank
    FROM batches
  ),
  latest AS (
    SELECT *,
      MAX(UNIX_SECONDS(_time_extracted)) OVER (PARTITION BY batch_id) as run_id
    FROM ranked_batches
    WHERE batch_rank = 1
  )
  SELECT * FROM latest;

  -- Step 2: Assign min/max dates using SET + scalar subqueries
  SET min_date = (
    SELECT MIN(DATE(start_at)) FROM latest_batch
  );

  SET max_date = (
    SELECT MAX(DATE(start_at)) FROM latest_batch
  );

  -- Step 3: Conditional delete and insert
  BEGIN TRANSACTION;

    IF EXISTS (
      SELECT 1
      FROM `{{target_dataset}}.{{target_table_id}}`
      WHERE day BETWEEN min_date AND max_date
        AND creative_id IN (
          SELECT DISTINCT creative_id FROM latest_batch
        )
      LIMIT 1
    ) THEN
      DELETE FROM `{{target_dataset}}.{{target_table_id}}`
      WHERE day BETWEEN min_date AND max_date
        AND creative_id IN (
          SELECT DISTINCT creative_id FROM latest_batch
        );
    END IF;

    INSERT INTO `{{target_dataset}}.{{target_table_id}}` (
      creative_id,
      day,
      _gn_id,
      creative,
      landing_page_clicks,
      document_completions,
      document_first_quartile_completions,
      document_midpoint_completions,
      document_third_quartile_completions,
      download_clicks,
      impressions,
      viral_document_completions,
      viral_document_first_quartile_completions,
      full_screen_plays,
      viral_document_midpoint_completions,
      viral_document_third_quartile_completions,
      viral_download_clicks,
      video_completions,
      card_clicks,
      card_impressions,
      comment_likes,
      viral_card_clicks,
      viral_card_impressions,
      viral_comment_likes,
      action_clicks,
      ad_unit_clicks,
      clicks,
      comments,
      company_page_clicks,
      conversion_value_in_local_currency,
      cost_in_local_currency,
      cost_in_usd,
      date_range,
      external_website_conversions,
      external_website_post_click_conversions,
      external_website_post_view_conversions,
      follows,
      lead_generation_mail_contact_info_shares,
      lead_generation_mail_interested_clicks,
      likes,
      one_click_lead_form_opens,
      one_click_leads,
      opens,
      other_engagements,
      pivot,
      pivot_value,
      pivot_values,
      reactions,
      sends,
      shares,
      text_url_clicks,
      total_engagements,
      video_first_quartile_completions,
      video_midpoint_completions,
      video_starts,
      video_third_quartile_completions,
      video_views,
      viral_clicks,
      viral_comments,
      viral_company_page_clicks,
      viral_follows,
      viral_full_screen_plays,
      viral_impressions,
      viral_landing_page_clicks,
      viral_likes,
      viral_one_click_lead_form_opens,
      viral_one_click_leads,
      viral_other_engagements,
      viral_reactions,
      viral_shares,
      viral_total_engagements,
      viral_video_completions,
      viral_video_first_quartile_completions,
      viral_video_midpoint_completions,
      viral_video_starts,
      viral_video_third_quartile_completions,
      viral_video_views,
      approximate_member_reach,
      tenant,
      run_id,
      _fivetran_synced,
      start_at,
      end_at,
      _time_extracted,
      _time_loaded,
      viral_external_website_conversions,
      viral_external_website_post_click_conversions,
      viral_external_website_post_view_conversions
    )
    SELECT
      SAFE_CAST(creative_id AS INT64) AS creative_id,
      DATE(start_at) AS day,
      TO_HEX(MD5(TO_JSON_STRING([
        CAST(creative_id AS STRING),
        CAST(DATE(start_at) AS STRING)
      ]))) AS _gn_id,
      creative,
      SAFE_CAST(landing_page_clicks AS INT64),
      SAFE_CAST(document_completions AS INT64),
      SAFE_CAST(document_first_quartile_completions AS INT64),
      SAFE_CAST(document_midpoint_completions AS INT64),
      SAFE_CAST(document_third_quartile_completions AS INT64),
      SAFE_CAST(download_clicks AS INT64),
      SAFE_CAST(impressions AS INT64),
      SAFE_CAST(viral_document_completions AS INT64),
      SAFE_CAST(viral_document_first_quartile_completions AS INT64),
      SAFE_CAST(full_screen_plays AS INT64),
      SAFE_CAST(viral_document_midpoint_completions AS INT64),
      SAFE_CAST(viral_document_third_quartile_completions AS INT64),
      SAFE_CAST(viral_download_clicks AS INT64),
      SAFE_CAST(video_completions AS INT64),
      SAFE_CAST(card_clicks AS INT64),
      SAFE_CAST(card_impressions AS INT64),
      SAFE_CAST(comment_likes AS INT64),
      SAFE_CAST(viral_card_clicks AS INT64),
      SAFE_CAST(viral_card_impressions AS INT64),
      SAFE_CAST(viral_comment_likes AS INT64),
      SAFE_CAST(action_clicks AS INT64),
      SAFE_CAST(ad_unit_clicks AS INT64),
      SAFE_CAST(clicks AS INT64),
      SAFE_CAST(comments AS INT64),
      SAFE_CAST(company_page_clicks AS INT64),
      SAFE_CAST(conversion_value_in_local_currency AS FLOAT64),
      SAFE_CAST(cost_in_local_currency AS FLOAT64),
      SAFE_CAST(cost_in_usd AS FLOAT64),
      date_range,
      SAFE_CAST(external_website_conversions AS INT64),
      SAFE_CAST(external_website_post_click_conversions AS INT64),
      SAFE_CAST(external_website_post_view_conversions AS INT64),
      SAFE_CAST(follows AS INT64),
      SAFE_CAST(lead_generation_mail_contact_info_shares AS INT64),
      SAFE_CAST(lead_generation_mail_interested_clicks AS INT64),
      SAFE_CAST(likes AS INT64),
      SAFE_CAST(one_click_lead_form_opens AS INT64),
      SAFE_CAST(one_click_leads AS INT64),
      SAFE_CAST(opens AS INT64),
      SAFE_CAST(other_engagements AS INT64),
      pivot,
      pivot_value,
      pivot_values,
      SAFE_CAST(reactions AS INT64),
      SAFE_CAST(sends AS INT64),
      SAFE_CAST(shares AS INT64),
      SAFE_CAST(text_url_clicks AS INT64),
      SAFE_CAST(total_engagements AS INT64),
      SAFE_CAST(video_first_quartile_completions AS INT64),
      SAFE_CAST(video_midpoint_completions AS INT64),
      SAFE_CAST(video_starts AS INT64),
      SAFE_CAST(video_third_quartile_completions AS INT64),
      SAFE_CAST(video_views AS INT64),
      SAFE_CAST(viral_clicks AS INT64),
      SAFE_CAST(viral_comments AS INT64),
      SAFE_CAST(viral_company_page_clicks AS INT64),
      SAFE_CAST(viral_follows AS INT64),
      SAFE_CAST(viral_full_screen_plays AS INT64),
      SAFE_CAST(viral_impressions AS INT64),
      SAFE_CAST(viral_landing_page_clicks AS INT64),
      SAFE_CAST(viral_likes AS INT64),
      SAFE_CAST(viral_one_click_lead_form_opens AS INT64),
      SAFE_CAST(viral_one_click_leads AS INT64),
      SAFE_CAST(viral_other_engagements AS INT64),
      SAFE_CAST(viral_reactions AS INT64),
      SAFE_CAST(viral_shares AS INT64),
      SAFE_CAST(viral_total_engagements AS INT64),
      SAFE_CAST(viral_video_completions AS INT64),
      SAFE_CAST(viral_video_first_quartile_completions AS INT64),
      SAFE_CAST(viral_video_midpoint_completions AS INT64),
      SAFE_CAST(viral_video_starts AS INT64),
      SAFE_CAST(viral_video_third_quartile_completions AS INT64),
      SAFE_CAST(viral_video_views AS INT64),
      SAFE_CAST(approximate_member_reach AS INT64),
      tenant,
      run_id,
      CURRENT_TIMESTAMP() AS _fivetran_synced,
      start_at,
      end_at,
      _time_extracted,
      _time_loaded,
      SAFE_CAST(viral_external_website_conversions AS INT64),
      SAFE_CAST(viral_external_website_post_click_conversions AS INT64),
      SAFE_CAST(viral_external_website_post_view_conversions AS INT64)
    FROM latest_batch;

  COMMIT TRANSACTION;

END IF; 