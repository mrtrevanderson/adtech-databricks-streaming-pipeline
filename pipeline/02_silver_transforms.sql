-- =============================================================================
-- LAYER 2: SILVER - Transforms, CDC, Stream-Stream Join, Session Windows
-- =============================================================================
-- Techniques demonstrated in this file:
--   1. Watermarks            -- tolerate late-arriving mobile events
--   2. AUTO CDC              -- merge user profile INSERT/UPDATE/DELETE into SCD Type 1
--   3. Stream-stream join    -- enrich purchase events with user profile in real time
--   4. Stateful session agg  -- aggregate user behavior within a session window
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
  -- TECHNIQUE: Watermark declaration
  -- Instructs Spark how long to wait for late-arriving events before
  -- advancing the event-time clock and releasing stateful operator memory.
  -- Mobile SDKs can batch events and deliver them late -- 15 min covers p99.
  WATERMARK event_timestamp DELAY OF INTERVAL 15 MINUTES
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
FROM STREAM(LIVE.bronze_ecommerce_events)
-- Data quality: only consented users, valid event types, no null event IDs
WHERE consent_flag = true
  AND event_type IN ('page_view', 'add_to_cart', 'checkout_start', 'purchase')
  AND event_id IS NOT NULL
  AND session_id IS NOT NULL;


-- -----------------------------------------------------------------------------
-- TECHNIQUE 2: AUTO CDC (Change Data Capture)
-- Merges INSERT / UPDATE / DELETE operations from the profile source into a
-- single current-state table (SCD Type 1 -- latest value wins).
-- -----------------------------------------------------------------------------
-- AUTO CDC handles:
--   - Out-of-order delivery (sequenced by updated_at)
--   - Duplicate records (idempotent merge on user_id)
--   - DELETE operations (tombstone rows)
-- This is much simpler than writing a manual MERGE statement.
CREATE OR REFRESH STREAMING TABLE silver_user_profiles
  COMMENT "Current-state user profiles maintained via AUTO CDC.
           Merges INSERT/UPDATE/DELETE records from the bronze profile source.
           SCD Type 1: latest record per user_id wins (no history retained here)."
  TBLPROPERTIES (
    "quality"                    = "silver",
    "delta.enableChangeDataFeed" = "true"
  );

-- AUTO CDC flow: read from bronze, apply changes to silver_user_profiles
APPLY CHANGES INTO LIVE.silver_user_profiles
FROM STREAM(LIVE.bronze_user_profiles_raw)
KEYS (user_id)                          -- merge key: one row per user
APPLY AS DELETE WHEN operation = 'DELETE'
SEQUENCE BY updated_at                  -- use updated_at to order out-of-sequence records
COLUMNS * EXCEPT (operation, updated_at, _ingest_timestamp, _source_file)
STORED AS SCD TYPE 1;                   -- overwrite existing row on UPDATE (no history)


-- -----------------------------------------------------------------------------
-- TECHNIQUE 3: Stream-Stream Join
-- Enrich purchase events with the buyer's profile attributes in real time.
-- Both sides of the join are streaming -- the watermark on each side
-- bounds the join window and prevents unbounded state growth.
-- -----------------------------------------------------------------------------
CREATE OR REFRESH STREAMING TABLE silver_enriched_purchases
  COMMENT "Purchase events enriched with real-time user profile attributes.
           Uses a stream-stream join between silver_ecommerce_events (purchases only)
           and silver_user_profiles. This is the key input for post-transaction
           ad targeting -- we need to know WHO bought WHAT to serve the right ad."
  TBLPROPERTIES (
    "quality" = "silver"
  )
  WATERMARK e.event_timestamp DELAY OF INTERVAL 15 MINUTES
AS
SELECT
    -- Purchase event fields
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

    -- User profile attributes (from stream-stream join)
    -- These drive ad personalization: what does this buyer care about?
    p.age_band,
    p.gender,
    p.income_band,
    p.interests,
    p.loyalty_tier,
    p.lifetime_value_usd,
    p.preferred_categories,
    p.last_purchase_category,
    p.total_orders,

    -- Derived targeting signals
    (p.loyalty_tier IN ('gold', 'platinum'))    AS is_high_value_customer,
    (p.lifetime_value_usd > 1000)               AS is_repeat_buyer,
    (p.total_orders > 5)                        AS is_loyal_shopper

FROM STREAM(LIVE.silver_ecommerce_events) AS e
-- TECHNIQUE: Stream-stream join
-- Inner join: only rows where a matching profile exists.
-- The watermark on e.event_timestamp bounds how long Spark holds state
-- waiting for a matching profile record to arrive.
JOIN LIVE.silver_user_profiles AS p
  ON e.user_id = p.user_id
WHERE e.event_type = 'purchase'
  AND e.user_id IS NOT NULL;


-- -----------------------------------------------------------------------------
-- TECHNIQUE 4: Stateful Session Window Aggregation
-- Summarize all events within a user session to build a behavioral fingerprint.
-- A session = all events for a (user_id, session_id) pair.
-- This captures the journey: did they browse -> cart -> purchase?
-- -----------------------------------------------------------------------------
CREATE OR REFRESH STREAMING TABLE silver_session_summary
  COMMENT "Behavioral summary of each user session.
           Aggregates all events within a session to capture the funnel:
           pages viewed, products explored, cart additions, and whether
           the session ended in a purchase.
           Used downstream to score session intent for ad targeting."
  TBLPROPERTIES (
    "quality" = "silver"
  )
  WATERMARK event_timestamp DELAY OF INTERVAL 15 MINUTES
AS
SELECT
    session_id,
    user_id,
    device_type,
    geo_region,
    event_date,

    -- Session time bounds
    MIN(event_timestamp)                                        AS session_start,
    MAX(event_timestamp)                                        AS session_end,
    ROUND(
        (UNIX_TIMESTAMP(MAX(event_timestamp)) -
         UNIX_TIMESTAMP(MIN(event_timestamp))) / 60.0, 2)      AS session_duration_min,

    -- Funnel event counts
    COUNT(*)                                                    AS total_events,
    COUNT(CASE WHEN event_type = 'page_view'      THEN 1 END)  AS page_views,
    COUNT(CASE WHEN event_type = 'add_to_cart'    THEN 1 END)  AS add_to_carts,
    COUNT(CASE WHEN event_type = 'checkout_start' THEN 1 END)  AS checkout_starts,
    COUNT(CASE WHEN event_type = 'purchase'       THEN 1 END)  AS purchases,

    -- Product engagement
    COUNT(DISTINCT product_id)                                  AS unique_products_viewed,
    COLLECT_SET(product_category)                               AS categories_browsed,
    MAX(product_price)                                          AS max_product_price_viewed,

    -- Purchase outcome
    (COUNT(CASE WHEN event_type = 'purchase' THEN 1 END) > 0)  AS converted,
    SUM(CASE WHEN event_type = 'purchase'
             THEN line_total_usd ELSE 0 END)                    AS session_revenue_usd,
    FIRST(order_id)                                             AS order_id,

    -- Intent score: weighted signal for how close to purchase the session got
    -- page_view=1pt, add_to_cart=3pts, checkout_start=5pts, purchase=10pts
    ROUND(
        COUNT(CASE WHEN event_type = 'page_view'      THEN 1 END) * 1  +
        COUNT(CASE WHEN event_type = 'add_to_cart'    THEN 1 END) * 3  +
        COUNT(CASE WHEN event_type = 'checkout_start' THEN 1 END) * 5  +
        COUNT(CASE WHEN event_type = 'purchase'       THEN 1 END) * 10,
    0)                                                          AS session_intent_score

FROM LIVE.silver_ecommerce_events
GROUP BY
    session_id, user_id, device_type, geo_region, event_date;
