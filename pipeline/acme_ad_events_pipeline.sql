-- =============================================================================
-- Acme Media Ad Events Pipeline - Silver & Gold Layers
-- =============================================================================
-- This file defines Silver and Gold tables using Lakeflow SDP SQL syntax.
-- Bronze ingestion is handled in acme_ad_events_pipeline.py (Auto Loader
-- requires Python). This file is loaded alongside the Python file in the
-- same Lakeflow pipeline.
-- =============================================================================


-- =============================================================================
-- LAYER 2: SILVER — Validated, deduplicated, consent-filtered events
-- =============================================================================

CREATE OR REFRESH STREAMING TABLE silver_ad_events
  COMMENT "Cleaned, deduplicated, consent-validated ad events.
           Rows failing quality checks are dropped and logged.
           Watermark set to tolerate 10-minute late arrivals."
  TBLPROPERTIES (
    "quality"                    = "silver",
    "delta.enableChangeDataFeed" = "true"
  )
AS
SELECT
    event_id,
    event_type,
    event_timestamp,
    consumer_id,
    campaign_id,
    advertiser_id,
    publisher_id,
    placement_id,
    bid_price_usd,
    COALESCE(revenue_usd, 0.0)           AS revenue_usd,
    consent_flag,
    geo_region,
    device_type,
    TO_DATE(event_timestamp)             AS event_date,
    DATE_TRUNC('HOUR', event_timestamp)  AS event_hour,
    (device_type = 'mobile')             AS is_mobile,
    _ingest_timestamp
FROM STREAM(LIVE.bronze_ad_events)
-- Watermark to tolerate late-arriving events up to 10 minutes
WATERMARK event_timestamp DELAY OF INTERVAL 10 MINUTES;


-- DLT data quality expectations on silver_ad_events
-- Rows failing any expectation are dropped and logged to the pipeline event log.
ALTER STREAMING TABLE silver_ad_events
  ADD CONSTRAINT valid_event_type  EXPECT (event_type IN ('click', 'impression', 'conversion')) ON VIOLATION DROP ROW;

ALTER STREAMING TABLE silver_ad_events
  ADD CONSTRAINT consent_required  EXPECT (consent_flag = true)   ON VIOLATION DROP ROW;

ALTER STREAMING TABLE silver_ad_events
  ADD CONSTRAINT non_null_event_id EXPECT (event_id IS NOT NULL)  ON VIOLATION DROP ROW;

ALTER STREAMING TABLE silver_ad_events
  ADD CONSTRAINT non_null_campaign EXPECT (campaign_id IS NOT NULL) ON VIOLATION DROP ROW;


-- =============================================================================
-- LAYER 3a: GOLD — Hourly campaign performance (feeds live dashboards)
-- =============================================================================

CREATE OR REFRESH MATERIALIZED VIEW gold_campaign_performance_hourly
  COMMENT "Hourly rollup of campaign KPIs: impressions, clicks, conversions, CTR, CVR, eCPM, revenue.
           Primary feed for the real-time campaign performance dashboard.
           Z-ordered on campaign_id and event_hour for fast BI queries."
  TBLPROPERTIES (
    "quality"                            = "gold",
    "pipelines.autoOptimize.zOrderCols"  = "campaign_id,event_hour"
  )
AS
SELECT
    campaign_id,
    advertiser_id,
    publisher_id,
    geo_region,
    device_type,
    event_hour,
    event_date,

    -- Volume metrics
    COUNT(CASE WHEN event_type = 'impression' THEN 1 END)  AS impressions,
    COUNT(CASE WHEN event_type = 'click'      THEN 1 END)  AS clicks,
    COUNT(CASE WHEN event_type = 'conversion' THEN 1 END)  AS conversions,

    -- Revenue metrics
    ROUND(SUM(revenue_usd), 4)                             AS total_revenue_usd,
    ROUND(AVG(bid_price_usd), 4)                           AS avg_bid_price_usd,

    -- Audience
    APPROX_COUNT_DISTINCT(consumer_id)                     AS unique_consumers,

    -- Derived KPIs
    ROUND(
        COUNT(CASE WHEN event_type = 'click' THEN 1 END) /
        NULLIF(COUNT(CASE WHEN event_type = 'impression' THEN 1 END), 0),
    4)                                                     AS ctr,

    ROUND(
        COUNT(CASE WHEN event_type = 'conversion' THEN 1 END) /
        NULLIF(COUNT(CASE WHEN event_type = 'click' THEN 1 END), 0),
    4)                                                     AS cvr,

    ROUND(
        SUM(revenue_usd) /
        NULLIF(COUNT(CASE WHEN event_type = 'impression' THEN 1 END), 0) * 1000,
    4)                                                     AS ecpm

FROM LIVE.silver_ad_events
GROUP BY
    campaign_id, advertiser_id, publisher_id,
    geo_region, device_type, event_hour, event_date;


-- =============================================================================
-- LAYER 3b: GOLD — Daily consumer engagement signals (feeds ML training)
-- =============================================================================

CREATE OR REFRESH MATERIALIZED VIEW gold_consumer_daily_signals
  COMMENT "Daily consumer-level engagement signals for ML audience model training.
           One row per (consumer_id, event_date).
           Feeds the nightly ML job that refreshes audience targeting models."
  TBLPROPERTIES (
    "quality"                            = "gold",
    "pipelines.autoOptimize.zOrderCols"  = "consumer_id,event_date"
  )
AS
SELECT
    consumer_id,
    event_date,

    -- Daily engagement counts
    COUNT(CASE WHEN event_type = 'impression' THEN 1 END)  AS daily_impressions,
    COUNT(CASE WHEN event_type = 'click'      THEN 1 END)  AS daily_clicks,
    COUNT(CASE WHEN event_type = 'conversion' THEN 1 END)  AS daily_conversions,

    -- Revenue
    ROUND(SUM(revenue_usd), 2)                             AS daily_revenue_usd,

    -- Behavioral signals
    COLLECT_SET(advertiser_id)                             AS advertisers_interacted,
    COLLECT_SET(geo_region)                                AS geo_regions_seen,
    FIRST(device_type)                                     AS primary_device,
    MAX(event_timestamp)                                   AS last_seen_at,

    -- Engagement score: weighted composite (replace with ML-derived feature in production)
    ROUND(
        COUNT(CASE WHEN event_type = 'click'      THEN 1 END) * 2.0  +
        COUNT(CASE WHEN event_type = 'conversion' THEN 1 END) * 10.0 +
        COUNT(CASE WHEN event_type = 'impression' THEN 1 END) * 0.1,
    2)                                                     AS engagement_score

FROM LIVE.silver_ad_events
WHERE consumer_id IS NOT NULL
GROUP BY consumer_id, event_date;
