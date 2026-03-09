-- =============================================================================
-- LAYER 3: GOLD - Post-Transaction Ad Targeting
-- =============================================================================
-- This is the business output layer: the data that drives Acme Media's
-- post-transaction ad serving engine.
--
-- After a user completes a purchase, two things need to happen fast:
--   1. Determine what offer to serve (based on profile + purchase context)
--   2. Update the user's aggregate targeting profile for future sessions
--
-- OFFER RECOMMENDATION LOGIC:
--   Priority order:
--     1. Complementary category to what was just purchased (highest signal)
--     2. User's stated preferred_categories (profile affinity)
--     3. User's interests (broader signal)
--     4. Fallback generic offer
-- =============================================================================


-- -----------------------------------------------------------------------------
-- GOLD 1: Post-Transaction Ad Triggers
-- One record per purchase event, ready for the ad serving engine to consume.
-- -----------------------------------------------------------------------------
CREATE OR REFRESH MATERIALIZED VIEW gold_post_transaction_triggers
  COMMENT "One record per completed purchase, enriched with personalized offer recommendations.
           Offer is selected based on purchase context + user interest affinity.
           After a user completes a purchase, this record tells the ad engine:
           who they are, what they bought, and what offer to serve next."
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

    -- Who bought it
    p.age_band,
    p.gender,
    p.income_band,
    p.loyalty_tier,
    p.lifetime_value_usd,
    p.interests,
    p.preferred_categories,
    p.is_high_value_customer,
    p.is_repeat_buyer,
    p.is_loyal_shopper,

    -- ---------------------------------------------------------------------
    -- OFFER RECOMMENDATION
    -- Step 1: derive the most relevant category for this user by combining
    --         purchase context with profile affinity signals.
    -- Step 2: map that category to a concrete offer string.
    --
    -- Logic:
    --   - If interests or preferred_categories mention the purchased category,
    --     the user has strong affinity -- serve a complementary offer in that
    --     same vertical (e.g. bought footwear + likes fitness = running gear deal)
    --   - Otherwise serve the top complementary category for what was bought
    --   - Loyalty tier upgrades the offer language for high-value customers
    -- ---------------------------------------------------------------------

    -- Derived affinity category: does the user's profile align with purchase?
    CASE
        WHEN LOWER(COALESCE(p.interests, '')) LIKE '%sport%'
          OR LOWER(COALESCE(p.preferred_categories, '')) LIKE '%sport%'
          OR LOWER(COALESCE(p.purchased_category, '')) LIKE '%sport%'
            THEN 'sports'
        WHEN LOWER(COALESCE(p.interests, '')) LIKE '%fitness%'
          OR LOWER(COALESCE(p.preferred_categories, '')) LIKE '%fitness%'
          OR p.purchased_category IN ('footwear', 'activewear')
            THEN 'fitness'
        WHEN LOWER(COALESCE(p.interests, '')) LIKE '%tech%'
          OR LOWER(COALESCE(p.preferred_categories, '')) LIKE '%tech%'
          OR LOWER(COALESCE(p.preferred_categories, '')) LIKE '%electron%'
          OR p.purchased_category = 'electronics'
            THEN 'tech'
        WHEN LOWER(COALESCE(p.interests, '')) LIKE '%beauty%'
          OR LOWER(COALESCE(p.interests, '')) LIKE '%skincare%'
          OR p.purchased_category = 'beauty'
            THEN 'beauty'
        WHEN LOWER(COALESCE(p.interests, '')) LIKE '%fashion%'
          OR p.purchased_category IN ('apparel', 'accessories', 'footwear')
            THEN 'fashion'
        WHEN LOWER(COALESCE(p.interests, '')) LIKE '%home%'
          OR p.purchased_category IN ('home', 'furniture', 'kitchen')
            THEN 'home'
        WHEN LOWER(COALESCE(p.interests, '')) LIKE '%travel%'
            THEN 'travel'
        ELSE 'general'
    END                                         AS affinity_category,

    -- Concrete offer recommendation based on affinity + loyalty tier
    CASE
        -- Sports affinity
        WHEN (LOWER(COALESCE(p.interests, '')) LIKE '%sport%'
           OR LOWER(COALESCE(p.preferred_categories, '')) LIKE '%sport%'
           OR LOWER(COALESCE(p.purchased_category, '')) LIKE '%sport%')
          AND p.loyalty_tier IN ('platinum', 'gold')
            THEN 'Exclusive: 20% off top sports equipment brands'
        WHEN (LOWER(COALESCE(p.interests, '')) LIKE '%sport%'
           OR LOWER(COALESCE(p.preferred_categories, '')) LIKE '%sport%'
           OR LOWER(COALESCE(p.purchased_category, '')) LIKE '%sport%')
            THEN 'Sports gear deal: save on your next equipment purchase'

        -- Fitness affinity
        WHEN (LOWER(COALESCE(p.interests, '')) LIKE '%fitness%'
           OR LOWER(COALESCE(p.preferred_categories, '')) LIKE '%fitness%'
           OR p.purchased_category IN ('footwear', 'activewear'))
          AND p.loyalty_tier IN ('platinum', 'gold')
            THEN 'VIP: Free shipping on activewear + fitness accessories'
        WHEN (LOWER(COALESCE(p.interests, '')) LIKE '%fitness%'
           OR LOWER(COALESCE(p.preferred_categories, '')) LIKE '%fitness%'
           OR p.purchased_category IN ('footwear', 'activewear'))
            THEN 'Complete your workout kit — activewear deals inside'

        -- Tech affinity
        WHEN (LOWER(COALESCE(p.interests, '')) LIKE '%tech%'
           OR LOWER(COALESCE(p.preferred_categories, '')) LIKE '%electron%'
           OR p.purchased_category = 'electronics')
          AND p.loyalty_tier IN ('platinum', 'gold')
            THEN 'Early access: new tech accessories at member pricing'
        WHEN (LOWER(COALESCE(p.interests, '')) LIKE '%tech%'
           OR LOWER(COALESCE(p.preferred_categories, '')) LIKE '%electron%'
           OR p.purchased_category = 'electronics')
            THEN 'Pair it up — accessories for your new device'

        -- Beauty affinity
        WHEN (LOWER(COALESCE(p.interests, '')) LIKE '%beauty%'
           OR LOWER(COALESCE(p.interests, '')) LIKE '%skincare%'
           OR p.purchased_category = 'beauty')
          AND p.loyalty_tier IN ('platinum', 'gold')
            THEN 'Luxury skincare set — exclusive member offer'
        WHEN (LOWER(COALESCE(p.interests, '')) LIKE '%beauty%'
           OR LOWER(COALESCE(p.interests, '')) LIKE '%skincare%'
           OR p.purchased_category = 'beauty')
            THEN 'Complete your routine — top beauty deals this week'

        -- Fashion affinity
        WHEN (LOWER(COALESCE(p.interests, '')) LIKE '%fashion%'
           OR p.purchased_category IN ('apparel', 'accessories', 'footwear'))
          AND p.loyalty_tier IN ('platinum', 'gold')
            THEN 'Style upgrade: member-exclusive fashion offers'
        WHEN (LOWER(COALESCE(p.interests, '')) LIKE '%fashion%'
           OR p.purchased_category IN ('apparel', 'accessories', 'footwear'))
            THEN 'Complete the look — matching accessories on sale'

        -- Home affinity
        WHEN (LOWER(COALESCE(p.interests, '')) LIKE '%home%'
           OR p.purchased_category IN ('home', 'furniture', 'kitchen'))
            THEN 'Home refresh deals — save on decor and essentials'

        -- Travel affinity
        WHEN LOWER(COALESCE(p.interests, '')) LIKE '%travel%'
            THEN 'Travel ready — gear and accessories for your next trip'

        -- High value fallback with no strong affinity
        WHEN p.loyalty_tier IN ('platinum', 'gold')
            THEN 'Exclusive member offer — top deals selected for you'

        -- Generic fallback
        ELSE 'Check out today''s top deals'
    END                                         AS recommended_offer,

    -- Bid price: how much to bid for this placement
    CASE p.loyalty_tier
        WHEN 'platinum' THEN 15.00
        WHEN 'gold'     THEN 10.00
        WHEN 'silver'   THEN  6.00
        ELSE                  3.00
    END                                         AS suggested_max_bid_usd,

    CURRENT_TIMESTAMP()                         AS targeting_generated_at

FROM LIVE.silver_enriched_purchases AS p;


-- -----------------------------------------------------------------------------
-- GOLD 2: User Aggregate Targeting Profile
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

    -- Category affinity
    FLATTEN(COLLECT_LIST(s.categories_browsed)) AS all_categories_browsed,

    -- Device + geo preference
    MODE(s.device_type)                         AS primary_device,
    MODE(s.geo_region)                          AS primary_geo,

    -- Recency
    MAX(s.session_end)                          AS last_active_at,
    MAX(s.event_date)                           AS last_active_date,

    -- Targeting tier
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
