# Acme Media Pipeline - Validation & Monitoring
# Run this notebook after starting the pipeline to verify data quality,
# row counts across layers, and inspect the Gold aggregates.

# ---------------------------------------------------------------------------
# 1. ROW COUNT AUDIT: Bronze -> Silver -> Gold
# Validates the expected funnel drop at each layer.
# ---------------------------------------------------------------------------
row_counts = spark.sql("""
    SELECT 'bronze_ad_events'                 AS layer, COUNT(*) AS row_count FROM acme_catalog.default.bronze_ad_events
    UNION ALL
    SELECT 'silver_ad_events'                 AS layer, COUNT(*) AS row_count FROM acme_catalog.default.silver_ad_events
    UNION ALL
    SELECT 'gold_campaign_performance_hourly' AS layer, COUNT(*) AS row_count FROM acme_catalog.default.gold_campaign_performance_hourly
    UNION ALL
    SELECT 'gold_consumer_daily_signals'      AS layer, COUNT(*) AS row_count FROM acme_catalog.default.gold_consumer_daily_signals
    ORDER BY layer
""")
display(row_counts)

# ---------------------------------------------------------------------------
# 2. SILVER DATA QUALITY CHECK
# Verify: no non-consented events, no duplicate event_ids, valid event types only.
# All checks should return 0 violations.
# ---------------------------------------------------------------------------
dq_checks = spark.sql("""
    SELECT 'non_consented_events' AS check_name, COUNT(*) AS violations
    FROM acme_catalog.default.silver_ad_events WHERE consent_flag = false
    UNION ALL
    SELECT 'invalid_event_types'  AS check_name, COUNT(*) AS violations
    FROM acme_catalog.default.silver_ad_events WHERE event_type NOT IN ('click','impression','conversion')
    UNION ALL
    SELECT 'null_event_ids'       AS check_name, COUNT(*) AS violations
    FROM acme_catalog.default.silver_ad_events WHERE event_id IS NULL
    UNION ALL
    SELECT 'duplicate_event_ids'  AS check_name, COUNT(*) - COUNT(DISTINCT event_id) AS violations
    FROM acme_catalog.default.silver_ad_events
""")
display(dq_checks)

# ---------------------------------------------------------------------------
# 3. LIVE CAMPAIGN PERFORMANCE (Gold Dashboard Feed)
# ---------------------------------------------------------------------------
campaign_perf = spark.sql("""
    SELECT
        campaign_id,
        advertiser_id,
        event_hour,
        impressions,
        clicks,
        conversions,
        ROUND(ctr * 100, 2)         AS ctr_pct,
        ROUND(cvr * 100, 2)         AS cvr_pct,
        ROUND(total_revenue_usd, 2) AS revenue_usd,
        ROUND(ecpm, 4)              AS ecpm,
        unique_consumers
    FROM acme_catalog.default.gold_campaign_performance_hourly
    ORDER BY event_hour DESC, total_revenue_usd DESC
    LIMIT 50
""")
display(campaign_perf)

# ---------------------------------------------------------------------------
# 4. TOP CONSUMERS BY ENGAGEMENT SCORE (ML Training Feed)
# ---------------------------------------------------------------------------
consumer_signals = spark.sql("""
    SELECT
        consumer_id,
        event_date,
        daily_impressions,
        daily_clicks,
        daily_conversions,
        ROUND(daily_revenue_usd, 2)         AS daily_revenue_usd,
        engagement_score,
        primary_device,
        SIZE(advertisers_interacted)        AS num_advertisers_seen
    FROM acme_catalog.default.gold_consumer_daily_signals
    ORDER BY engagement_score DESC
    LIMIT 25
""")
display(consumer_signals)

# ---------------------------------------------------------------------------
# 5. PIPELINE EVENT LOG - Check for errors or dropped records
# DLT logs expectation failures (rows dropped in Silver) to the event log.
# ---------------------------------------------------------------------------
event_log = spark.sql("""
    SELECT
        timestamp,
        event_type,
        details:flow_name       AS flow_name,
        details:expectations    AS expectation_results,
        details:num_output_rows AS output_rows,
        details:dropped_records AS dropped_records
    FROM acme_catalog.default.`acme_ad_pipeline_events`
    WHERE event_type IN ('flow_progress', 'flow_definition')
    ORDER BY timestamp DESC
    LIMIT 100
""")
display(event_log)
