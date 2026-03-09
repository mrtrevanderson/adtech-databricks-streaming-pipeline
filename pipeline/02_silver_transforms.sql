-- =============================================================================
-- LAYER 2: SILVER - Transforms, CDC, Stream-Stream Join, Session Windows
-- =============================================================================
-- Techniques demonstrated in this file:
--   1. Watermarks            -- tolerate late-arriving mobile events
--   2. AUTO CDC              -- merge user profile INSERT/UPDATE/DELETE into SCD Type 1
--   3. Stream-stream join    -- enrich purchase events with user profile in real time
--   4. Stateful session agg  -- aggregate user behavior within a session window
--
-- WATERMARK SYNTAX NOTE:
--   The WATERMARK clause belongs in the FROM clause, NOT the CREATE TABLE header.
--   Correct pattern:
--     FROM STREAM(source) WATERMARK timestamp_col DELAY OF INTERVAL N MINUTES AS alias
-- =============================================================================


-- -----------------------------------------------------------------------------
-- TECHNIQUE 1: Watermarks + Data Quality Expectations
-- Clean, consent-filtered event stream with watermark for late data tolerance.
-- -----------------------------------------------------------------------------
CREATE OR REFRESH STREAMING TABLE silver_ecommerce_events
  COMMENT "Cleaned and consent-validated e-commerce events.
           Watermark set to 15 minutes to tolerate late mobile SDK events.
           Rows failing quality checks are dropped and logged."
  TBLPROPERTIES (
    "quality"                    = "silver",
    "delta.enableChangeDataFeed" = "true"
  )
AS
SELECT
    event_id,
    event_timestamp,
    session_id,
    user_id,
    event_type,
    page_url,
    product_id,
    product_category,
    COALESCE(product_price, 0.0)                    AS product_price,
    COALESCE(quantity, 1)                           AS quantity,
    COALESCE(product_price * quantity, 0.0)         AS line_total_usd,
    order_id,
    consent_flag,
    device_type,
    geo_region,
    TO_DATE(event_timestamp)                        AS event_date,
    DATE_TRUNC('HOUR', event_timestamp)             AS event_hour,
    (device_type = 'mobile')                        AS is_mobile,
    (event_type = 'purchase')                       AS is_purchase,
    _ingest_timestamp
-- TECHNIQUE: WATERMARK goes in the FROM clause, not the CREATE header.
-- Instructs Spark how long to wait for late-arriving events before
-- advancing the event-time clock and releasing stateful operator memory.
-- Mobile SDKs can batch events and deliver them late -- 15 min covers p99.
FROM STREAM(LIVE.bronze_ecommerce_events)
    WATERMARK event_timestamp DELAY OF INTERVAL 15 MINUTES
WHERE consent_flag = true
  AND event_type IN ('page_view', 'add_to_cart', 'checkout_start', 'purchase')
  AND event_id IS NOT NULL
  AND session_id IS NOT NULL;


-- -----------------------------------------------------------------------------
-- TECHNIQUE 2: AUTO CDC (Change Data Capture)
-- Merges INSERT / UPDATE / DELETE operations from the profile source into a
-- single current-state table (SCD Type 1 -- latest value wins).
-- -----------------------------------------------------------------------------
CREATE OR REFRESH STREAMING TABLE silver_user_profiles
  COMMENT "Current-state user profiles maintained via AUTO CDC.
           Merges INSERT/UPDATE/DELETE records from the bronze profile source.
           SCD Type 1: latest record per user_id wins (no history retained here)."
  TBLPROPERTIES (
    "quality"                    = "silver",
    "delta.enableChangeDataFeed" = "true"
  );

APPLY CHANGES INTO LIVE.silver_user_profiles
FROM STREAM(LIVE.bronze_user_profiles_raw)
KEYS (user_id)
APPLY AS DELETE WHEN operation = 'DELETE'
SEQUENCE BY updated_at
-- Keep updated_at so it can be used as the watermark in the stream-stream join downstream
COLUMNS * EXCEPT (operation, _ingest_timestamp, _source_file)
STORED AS SCD TYPE 1;


-- -----------------------------------------------------------------------------
-- TECHNIQUE 3: Stream-Stream Join
-- Enrich purchase events with the buyer's profile attributes in real time.
-- -----------------------------------------------------------------------------
CREATE OR REFRESH STREAMING TABLE silver_enriched_purchases
  COMMENT "Purchase events enriched with real-time user profile attributes.
           Stream-stream join between silver_ecommerce_events and silver_user_profiles.
           WATERMARK on both sides bounds state and prevents OOM."
  TBLPROPERTIES (
    "quality" = "silver"
  )
AS
SELECT
    e.event_id,
    e.event_timestamp,
    e.session_id,
    e.user_id,
    e.order_id,
    e.product_id,
    e.product_category         AS purchased_category,
    e.product_price,
    e.quantity,
    e.line_total_usd,
    e.device_type,
    e.geo_region,
    e.event_date,
    p.age_band,
    p.gender,
    p.income_band,
    p.interests,
    p.loyalty_tier,
    p.lifetime_value_usd,
    p.preferred_categories,
    p.last_purchase_category,
    p.total_orders,
    (p.loyalty_tier IN ('gold', 'platinum'))    AS is_high_value_customer,
    (p.lifetime_value_usd > 1000)               AS is_repeat_buyer,
    (p.total_orders > 5)                        AS is_loyal_shopper
-- TECHNIQUE: WATERMARK on both sides of a stream-stream join.
-- Required by Spark to bound state on each side of the join.
-- Without it the engine has no way to know when to expire unmatched rows.
FROM STREAM(LIVE.silver_ecommerce_events)
    WATERMARK event_timestamp DELAY OF INTERVAL 15 MINUTES AS e
JOIN STREAM(LIVE.silver_user_profiles)
    WATERMARK updated_at DELAY OF INTERVAL 30 MINUTES AS p
  ON e.user_id = p.user_id
WHERE e.event_type = 'purchase'
  AND e.user_id IS NOT NULL;


-- -----------------------------------------------------------------------------
-- TECHNIQUE 4: Stateful Session Window Aggregation
-- -----------------------------------------------------------------------------
CREATE OR REFRESH STREAMING TABLE silver_session_summary
  COMMENT "Behavioral summary per session: funnel counts, intent score, revenue.
           Used to score session intent for ad targeting."
  TBLPROPERTIES (
    "quality" = "silver"
  )
AS
SELECT
    session_id,
    user_id,
    device_type,
    geo_region,
    event_date,
    MIN(event_timestamp)                                        AS session_start,
    MAX(event_timestamp)                                        AS session_end,
    ROUND(
        (UNIX_TIMESTAMP(MAX(event_timestamp)) -
         UNIX_TIMESTAMP(MIN(event_timestamp))) / 60.0, 2)      AS session_duration_min,
    COUNT(*)                                                    AS total_events,
    COUNT(CASE WHEN event_type = 'page_view'      THEN 1 END)  AS page_views,
    COUNT(CASE WHEN event_type = 'add_to_cart'    THEN 1 END)  AS add_to_carts,
    COUNT(CASE WHEN event_type = 'checkout_start' THEN 1 END)  AS checkout_starts,
    COUNT(CASE WHEN event_type = 'purchase'       THEN 1 END)  AS purchases,
    APPROX_COUNT_DISTINCT(product_id)                           AS unique_products_viewed,
    APPROX_COUNT_DISTINCT(product_category)                     AS categories_browsed,
    MAX(product_price)                                          AS max_product_price_viewed,
    (COUNT(CASE WHEN event_type = 'purchase' THEN 1 END) > 0)  AS converted,
    SUM(CASE WHEN event_type = 'purchase'
             THEN line_total_usd ELSE 0 END)                    AS session_revenue_usd,
    FIRST(order_id)                                             AS order_id,
    ROUND(
        COUNT(CASE WHEN event_type = 'page_view'      THEN 1 END) * 1  +
        COUNT(CASE WHEN event_type = 'add_to_cart'    THEN 1 END) * 3  +
        COUNT(CASE WHEN event_type = 'checkout_start' THEN 1 END) * 5  +
        COUNT(CASE WHEN event_type = 'purchase'       THEN 1 END) * 10,
    0)                                                          AS session_intent_score
-- TECHNIQUE: WATERMARK required for stateful GROUP BY aggregation.
-- Tells Spark when a session window is complete so it can emit and evict state.
FROM STREAM(LIVE.silver_ecommerce_events)
    WATERMARK event_timestamp DELAY OF INTERVAL 15 MINUTES
GROUP BY
    session_id, user_id, device_type, geo_region, event_date;
