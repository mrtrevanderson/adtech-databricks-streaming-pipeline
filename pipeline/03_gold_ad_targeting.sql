-- =============================================================================
-- LAYER 3: GOLD - Post-Transaction Ad Targeting
-- =============================================================================
-- This is the business output layer: the data that drives Acme Media's
-- post-transaction ad serving engine.
--
-- After a user completes a purchase, two things need to happen fast:
--   1. Determine what ad to serve (based on profile + purchase context)
--   2. Update the user's aggregate targeting profile for future sessions
-- =============================================================================


-- -----------------------------------------------------------------------------
-- GOLD 1: Post-Transaction Ad Triggers
-- One record per purchase event, ready for the ad serving engine to consume.
-- This is the "serve an ad RIGHT NOW" output -- latency matters here.
-- -----------------------------------------------------------------------------
CREATE OR REFRESH MATERIALIZED VIEW gold_post_transaction_triggers
  COMMENT "One record per completed purchase, enriched with ad targeting signals.
           This is the primary feed for the post-transaction ad serving engine.
           After a user completes a purchase, this record tells the ad engine:
           who they are, what they bought, and what ad category to serve next."
  TBLPROPERTIES (
    "quality"                           = "gold",
    "pipelines.autoOptimize.zOrderCols" = "user_id,event_date"
  )
AS
SELECT
    -- Transaction identity
    p.event_id,
    p.order_id,
    p.user_id,
    p.session_id,
    p.event_timestamp                           AS purchase_timestamp,
    p.event_date,

    -- What was purchased
    p.product_id,
    p.purchased_category,
    p.product_price,
    p.quantity,
    p.line_total_usd,
    p.device_type,
    p.geo_region,

    -- Who bought it (profile attributes for targeting)
    p.age_band,
    p.gender,
    p.income_band,
    p.loyalty_tier,
    p.lifetime_value_usd,
    p.is_high_value_customer,
    p.is_repeat_buyer,
    p.is_loyal_shopper,

    -- Ad targeting recommendation
    -- Logic: suggest complementary categories based on what was just purchased
    CASE p.purchased_category
        WHEN 'footwear'     THEN 'activewear,fitness,socks'
        WHEN 'apparel'      THEN 'accessories,footwear,bags'
        WHEN 'accessories'  THEN 'apparel,footwear,jewelry'
        WHEN 'electronics'  THEN 'accessories,tech_accessories,cables'
        WHEN 'beauty'       THEN 'skincare,wellness,fragrance'
        ELSE p.preferred_categories
    END                                         AS recommended_ad_categories,

    -- Bid price signal: how much should we bid to serve this user an ad?
    -- Higher value customers get higher bids for premium placements
    CASE p.loyalty_tier
        WHEN 'platinum' THEN 15.00
        WHEN 'gold'     THEN 10.00
        WHEN 'silver'   THEN  6.00
        ELSE                  3.00
    END                                         AS suggested_max_bid_usd,

    -- Ad format recommendation based on device
    CASE p.device_type
        WHEN 'mobile'  THEN 'interstitial'
        WHEN 'tablet'  THEN 'banner_large'
        ELSE                'banner_standard'
    END                                         AS recommended_ad_format,

    -- Personalization context for the ad creative
    p.interests,
    p.preferred_categories,
    p.total_orders,
    CURRENT_TIMESTAMP()                         AS targeting_generated_at

FROM LIVE.silver_enriched_purchases AS p;


-- -----------------------------------------------------------------------------
-- GOLD 2: User Aggregate Targeting Profile
-- Rolled-up behavioral profile per user across all sessions.
-- Updated after every session -- feeds identity graph and ML models.
-- -----------------------------------------------------------------------------
CREATE OR REFRESH MATERIALIZED VIEW gold_user_targeting_profile
  COMMENT "Aggregate behavioral profile per user across all sessions and purchases.
           Combines session summaries with purchase history to build a rich
           targeting profile used by the ML audience model and identity graph."
  TBLPROPERTIES (
    "quality"                           = "gold",
    "pipelines.autoOptimize.zOrderCols" = "user_id"
  )
AS
SELECT
    s.user_id,

    -- Session behavior aggregates
    COUNT(DISTINCT s.session_id)                AS total_sessions,
    ROUND(AVG(s.session_duration_min), 2)       AS avg_session_duration_min,
    SUM(s.page_views)                           AS total_page_views,
    SUM(s.add_to_carts)                         AS total_add_to_carts,
    SUM(s.purchases)                            AS total_purchases,
    ROUND(AVG(s.session_intent_score), 1)       AS avg_intent_score,

    -- Conversion metrics
    ROUND(
        SUM(CASE WHEN s.converted THEN 1 ELSE 0 END) * 100.0
        / NULLIF(COUNT(DISTINCT s.session_id), 0),
    2)                                          AS session_conversion_rate_pct,

    -- Revenue
    ROUND(SUM(s.session_revenue_usd), 2)        AS total_revenue_usd,
    ROUND(AVG(s.session_revenue_usd), 2)        AS avg_order_value_usd,

    -- Category affinity (what do they browse and buy?)
    FLATTEN(COLLECT_LIST(s.categories_browsed)) AS all_categories_browsed,

    -- Device + geo preference
    MODE(s.device_type)                         AS primary_device,
    MODE(s.geo_region)                          AS primary_geo,

    -- Recency
    MAX(s.session_end)                          AS last_active_at,
    MAX(s.event_date)                           AS last_active_date,

    -- Targeting tier based on behavior
    CASE
        WHEN SUM(s.session_revenue_usd) > 500   THEN 'high_value'
        WHEN SUM(s.session_revenue_usd) > 100   THEN 'mid_value'
        WHEN SUM(s.purchases) > 0               THEN 'converted'
        WHEN AVG(s.session_intent_score) > 5    THEN 'high_intent'
        ELSE                                         'browsing'
    END                                         AS behavioral_targeting_tier

FROM LIVE.silver_session_summary AS s
WHERE s.user_id IS NOT NULL
GROUP BY s.user_id;
