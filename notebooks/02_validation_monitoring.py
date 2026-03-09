# Acme Media Pipeline - Validation & Monitoring
# ================================================
# Run after starting the pipeline to verify each layer is working correctly.
# Organized by pipeline layer: Bronze -> Silver -> Gold

# ---------------------------------------------------------------------------
# SETUP: Confirm tables exist and have data
# ---------------------------------------------------------------------------
row_counts = spark.sql("""
    SELECT 'bronze_ecommerce_events'          AS layer_table, COUNT(*) AS rows FROM acme_catalog.acme_ad_pipeline.bronze_ecommerce_events
    UNION ALL
    SELECT 'bronze_user_profiles_raw'         AS layer_table, COUNT(*) AS rows FROM acme_catalog.acme_ad_pipeline.bronze_user_profiles_raw
    UNION ALL
    SELECT 'silver_ecommerce_events'          AS layer_table, COUNT(*) AS rows FROM acme_catalog.acme_ad_pipeline.silver_ecommerce_events
    UNION ALL
    SELECT 'silver_user_profiles'             AS layer_table, COUNT(*) AS rows FROM acme_catalog.acme_ad_pipeline.silver_user_profiles
    UNION ALL
    SELECT 'silver_enriched_purchases'        AS layer_table, COUNT(*) AS rows FROM acme_catalog.acme_ad_pipeline.silver_enriched_purchases
    UNION ALL
    SELECT 'silver_session_summary'           AS layer_table, COUNT(*) AS rows FROM acme_catalog.acme_ad_pipeline.silver_session_summary
    UNION ALL
    SELECT 'gold_post_transaction_triggers'   AS layer_table, COUNT(*) AS rows FROM acme_catalog.acme_ad_pipeline.gold_post_transaction_triggers
    UNION ALL
    SELECT 'gold_user_targeting_profile'      AS layer_table, COUNT(*) AS rows FROM acme_catalog.acme_ad_pipeline.gold_user_targeting_profile
    ORDER BY layer_table
""")
display(row_counts)

# ---------------------------------------------------------------------------
# BRONZE: Verify event type distribution looks realistic
# Expected funnel: page_view >> add_to_cart > checkout_start > purchase
# ---------------------------------------------------------------------------
bronze_funnel = spark.sql("""
    SELECT
        event_type,
        COUNT(*)                                    AS total_events,
        ROUND(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER(), 1) AS pct
    FROM acme_catalog.acme_ad_pipeline.bronze_ecommerce_events
    GROUP BY event_type
    ORDER BY total_events DESC
""")
display(bronze_funnel)

# ---------------------------------------------------------------------------
# SILVER: Data quality checks
# All violation counts should be 0
# ---------------------------------------------------------------------------
dq_checks = spark.sql("""
    SELECT 'non_consented_rows'  AS check_name, COUNT(*) AS violations
    FROM acme_catalog.acme_ad_pipeline.silver_ecommerce_events
    WHERE consent_flag = false
    UNION ALL
    SELECT 'invalid_event_types' AS check_name, COUNT(*) AS violations
    FROM acme_catalog.acme_ad_pipeline.silver_ecommerce_events
    WHERE event_type NOT IN ('page_view','add_to_cart','checkout_start','purchase')
    UNION ALL
    SELECT 'null_session_ids'    AS check_name, COUNT(*) AS violations
    FROM acme_catalog.acme_ad_pipeline.silver_ecommerce_events
    WHERE session_id IS NULL
    UNION ALL
    SELECT 'null_event_ids'      AS check_name, COUNT(*) AS violations
    FROM acme_catalog.acme_ad_pipeline.silver_ecommerce_events
    WHERE event_id IS NULL
""")
display(dq_checks)

# ---------------------------------------------------------------------------
# SILVER: Verify AUTO CDC merged profiles correctly
# Each user_id should appear exactly once (SCD Type 1)
# ---------------------------------------------------------------------------
profile_cdc_check = spark.sql("""
    SELECT
        COUNT(*)                        AS total_profiles,
        COUNT(DISTINCT user_id)         AS unique_users,
        COUNT(*) - COUNT(DISTINCT user_id) AS duplicate_users,
        MIN(loyalty_tier)               AS min_tier,
        MAX(lifetime_value_usd)         AS max_ltv
    FROM acme_catalog.acme_ad_pipeline.silver_user_profiles
""")
display(profile_cdc_check)

# ---------------------------------------------------------------------------
# SILVER: Stream-stream join -- enriched purchases
# Verify profile attributes attached to purchase events
# ---------------------------------------------------------------------------
enriched_purchases = spark.sql("""
    SELECT
        order_id,
        user_id,
        purchased_category,
        product_price,
        loyalty_tier,
        age_band,
        is_high_value_customer,
        is_repeat_buyer,
        recommended_ad_categories,
        purchase_timestamp
    FROM acme_catalog.acme_ad_pipeline.gold_post_transaction_triggers
    ORDER BY purchase_timestamp DESC
    LIMIT 20
""")
display(enriched_purchases)

# ---------------------------------------------------------------------------
# SILVER: Session summary -- check funnel conversion
# ---------------------------------------------------------------------------
session_funnel = spark.sql("""
    SELECT
        converted,
        COUNT(*)                            AS sessions,
        ROUND(AVG(session_duration_min), 1) AS avg_duration_min,
        ROUND(AVG(session_intent_score), 1) AS avg_intent_score,
        ROUND(AVG(page_views), 1)           AS avg_page_views,
        ROUND(SUM(session_revenue_usd), 2)  AS total_revenue
    FROM acme_catalog.acme_ad_pipeline.silver_session_summary
    GROUP BY converted
    ORDER BY converted DESC
""")
display(session_funnel)

# ---------------------------------------------------------------------------
# GOLD: Ad targeting recommendations by loyalty tier
# ---------------------------------------------------------------------------
targeting_summary = spark.sql("""
    SELECT
        loyalty_tier,
        COUNT(*)                                AS purchase_events,
        ROUND(AVG(suggested_max_bid_usd), 2)    AS avg_bid_usd,
        ROUND(AVG(product_price), 2)            AS avg_order_value,
        recommended_ad_format,
        COUNT(DISTINCT user_id)                 AS unique_buyers
    FROM acme_catalog.acme_ad_pipeline.gold_post_transaction_triggers
    GROUP BY loyalty_tier, recommended_ad_format
    ORDER BY avg_bid_usd DESC
""")
display(targeting_summary)

# ---------------------------------------------------------------------------
# GOLD: User behavioral targeting tiers
# ---------------------------------------------------------------------------
behavioral_tiers = spark.sql("""
    SELECT
        behavioral_targeting_tier,
        COUNT(*)                                AS users,
        ROUND(AVG(total_revenue_usd), 2)        AS avg_ltv,
        ROUND(AVG(avg_order_value_usd), 2)      AS avg_order_value,
        ROUND(AVG(session_conversion_rate_pct), 1) AS avg_cvr_pct,
        ROUND(AVG(avg_intent_score), 1)         AS avg_intent_score
    FROM acme_catalog.acme_ad_pipeline.gold_user_targeting_profile
    GROUP BY behavioral_targeting_tier
    ORDER BY avg_ltv DESC
""")
display(behavioral_tiers)
