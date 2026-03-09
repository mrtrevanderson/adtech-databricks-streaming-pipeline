"""
Acme Media Ad Events Streaming Pipeline
====================================
Simulates Acme Media's real-time ad event ingestion pipeline using:
 - Spark Structured Streaming (Auto Loader for ingestion)
 - Lakeflow Spark Declarative Pipelines (SDP)
 - Medallion Architecture: Bronze Silver Gold
 - Delta Lake for ACID storage

Business Context (Acme Media):
 Acme Media processes ad click, impression, and conversion events across
 its 200M+ first-party consumer profiles. This pipeline:
 1. Ingests raw click/impression events from a cloud landing zone
 2. Deduplicates and enriches events in Silver
 3. Produces Gold-layer aggregates for:
 - Live dashboards (real-time campaign performance)
 - ML training jobs (daily audience model refresh)

Architecture: Kafka/S3 Bronze (raw) Silver (clean/deduped) Gold (aggregated)
"""

import dlt
from pyspark.sql import functions as F
from pyspark.sql.types import (
 StructType, StructField, StringType, LongType,
 DoubleType, TimestampType, BooleanType
)

# ---------------------------------------------------------------------------
# CONFIG
# ---------------------------------------------------------------------------
# In a real Acme Media deployment these would be Databricks secrets / job params
RAW_EVENTS_PATH = "/Volumes/acme_catalog/raw/ad_events/" # Auto Loader source
SCHEMA_HINTS_PATH = "/Volumes/acme_catalog/raw/schema_hints/" # schema inference cache
WATERMARK_DELAY = "10 minutes" # tolerate late-arriving events up to 10 min
DEDUP_WINDOW = "1 hour" # dedup window for click events

# ---------------------------------------------------------------------------
# SCHEMA (declared explicitly for production-grade inference)
# ---------------------------------------------------------------------------
ad_event_schema = StructType([
 StructField("event_id", StringType(), False), # UUID
 StructField("event_type", StringType(), False), # click | impression | conversion
 StructField("event_timestamp", TimestampType(), False), # UTC epoch ms cast to TS
 StructField("consumer_id", StringType(), True), # Acme Media first-party profile ID
 StructField("campaign_id", StringType(), False),
 StructField("advertiser_id", StringType(), False),
 StructField("publisher_id", StringType(), True),
 StructField("placement_id", StringType(), True),
 StructField("bid_price_usd", DoubleType(), True), # winning bid (impressions only)
 StructField("revenue_usd", DoubleType(), True), # CPA/CPL revenue (conversions)
 StructField("user_agent", StringType(), True),
 StructField("ip_hash", StringType(), True), # PII-safe hashed IP
 StructField("consent_flag", BooleanType(), False), # CCPA/GDPR consent
 StructField("geo_region", StringType(), True), # US state code
 StructField("device_type", StringType(), True), # mobile | desktop | tablet
])

# ===========================================================================
# LAYER 1: BRONZE Raw ingestion via Auto Loader
# ===========================================================================
@dlt.table(
 name="bronze_ad_events",
 comment="Raw ad events landed from upstream producers (Kafka S3 Auto Loader). "
 "No transformations applied append-only, full fidelity.",
 table_properties={
 "quality": "bronze",
 "pipelines.autoOptimize.managed": "true",
 }
)
def bronze_ad_events():
 """
 Auto Loader reads JSON files dropped into the landing zone.
 cloudFiles format handles schema inference, evolution, and
 exactly-once ingestion with checkpointing.
 """
 return (
 spark.readStream
 .format("cloudFiles")
 .option("cloudFiles.format", "json")
 .option("cloudFiles.schemaLocation", SCHEMA_HINTS_PATH)
 .option("cloudFiles.inferColumnTypes", "true")
 .option("cloudFiles.schemaEvolutionMode", "addNewColumns")
 .schema(ad_event_schema)
 .load(RAW_EVENTS_PATH)
 # Capture ingestion metadata for lineage / late-data auditing
 .withColumn("_ingest_timestamp", F.current_timestamp())
 .withColumn("_source_file", F.col("_metadata.file_path"))
 )


# ===========================================================================
# LAYER 2: SILVER Validated, deduplicated, consent-filtered events
# ===========================================================================
@dlt.expect_or_drop("valid_event_type", "event_type IN ('click', 'impression', 'conversion')")
@dlt.expect_or_drop("consent_required", "consent_flag = true")
@dlt.expect_or_drop("non_null_event_id", "event_id IS NOT NULL")
@dlt.expect_or_drop("non_null_campaign", "campaign_id IS NOT NULL")
@dlt.table(
 name="silver_ad_events",
 comment="Cleaned, deduplicated, consent-validated ad events. "
 "Rows failing quality checks are dropped (DLT expectation). "
 "Watermark set to tolerate 10-minute late arrivals.",
 table_properties={
 "quality": "silver",
 "delta.enableChangeDataFeed": "true", # needed for downstream CDC / ML
 }
)
def silver_ad_events():
 """
 Key transformations:
 1. Watermark for late-data tolerance
 2. Dedup on (event_id) within a 1-hour window critical for click fraud prevention
 3. Derived columns: event_date partition key, revenue normalization
 4. Drop PII-adjacent fields not needed downstream
 """
 return (
 dlt.read_stream("bronze_ad_events")
 # --- Watermark for stateful dedup ---
 .withWatermark("event_timestamp", WATERMARK_DELAY)
 # --- Deduplication: same event_id may arrive multiple times from retry logic ---
 .dropDuplicates(["event_id"])
 # --- Derived fields ---
 .withColumn("event_date",
 F.to_date("event_timestamp"))
 .withColumn("event_hour",
 F.date_trunc("hour", F.col("event_timestamp")))
 .withColumn("revenue_usd",
 F.coalesce(F.col("revenue_usd"), F.lit(0.0)))
 .withColumn("is_mobile",
 F.col("device_type") == "mobile")
 # --- Drop raw ingestion metadata ---
 .drop("_source_file", "user_agent", "ip_hash")
 )


# ===========================================================================
# LAYER 3a: GOLD Hourly campaign performance (feeds live dashboards)
# ===========================================================================
@dlt.table(
 name="gold_campaign_performance_hourly",
 comment="Hourly rollup of campaign KPIs: impressions, clicks, conversions, CTR, CVR, revenue. "
 "Primary feed for Acme Media's real-time campaign performance dashboard.",
 table_properties={
 "quality": "gold",
 "pipelines.autoOptimize.zOrderCols": "campaign_id,event_hour",
 }
)
def gold_campaign_performance_hourly():
 """
 Materialized view (batch refresh) over silver_ad_events.
 Aggregated at (campaign_id, advertiser_id, event_hour) grain.
 """
 return (
 dlt.read("silver_ad_events")
 .groupBy("campaign_id", "advertiser_id", "publisher_id",
 "geo_region", "device_type", "event_hour", "event_date")
 .agg(
 F.count(F.when(F.col("event_type") == "impression", 1)).alias("impressions"),
 F.count(F.when(F.col("event_type") == "click", 1)).alias("clicks"),
 F.count(F.when(F.col("event_type") == "conversion", 1)).alias("conversions"),
 F.sum("revenue_usd").alias("total_revenue_usd"),
 F.avg("bid_price_usd").alias("avg_bid_price_usd"),
 F.approx_count_distinct("consumer_id").alias("unique_consumers"),
 )
 # --- Derived KPIs ---
 .withColumn("ctr", # Click-through rate
 F.round(F.col("clicks") / F.nullif(F.col("impressions"), F.lit(0)), 4))
 .withColumn("cvr", # Conversion rate
 F.round(F.col("conversions") / F.nullif(F.col("clicks"), F.lit(0)), 4))
 .withColumn("ecpm", # Effective CPM
 F.round((F.col("total_revenue_usd") / F.nullif(F.col("impressions"), F.lit(0))) * 1000, 4))
 )


# ===========================================================================
# LAYER 3b: GOLD Daily consumer engagement (feeds ML training)
# ===========================================================================
@dlt.table(
 name="gold_consumer_daily_signals",
 comment="Daily consumer-level engagement signals for ML audience model training. "
 "One row per (consumer_id, event_date). Used by Acme Media's targeting ML pipeline.",
 table_properties={
 "quality": "gold",
 "pipelines.autoOptimize.zOrderCols": "consumer_id,event_date",
 }
)
def gold_consumer_daily_signals():
 """
 Aggregated at (consumer_id, event_date) grain.
 Output feeds the nightly ML training job that refreshes Acme Media's
 audience targeting models (identity graph scoring, propensity models).
 """
 return (
 dlt.read("silver_ad_events")
 .filter(F.col("consumer_id").isNotNull())
 .groupBy("consumer_id", "event_date")
 .agg(
 F.count(F.when(F.col("event_type") == "impression", 1)).alias("daily_impressions"),
 F.count(F.when(F.col("event_type") == "click", 1)).alias("daily_clicks"),
 F.count(F.when(F.col("event_type") == "conversion", 1)).alias("daily_conversions"),
 F.sum("revenue_usd").alias("daily_revenue_usd"),
 F.collect_set("advertiser_id").alias("advertisers_interacted"),
 F.collect_set("geo_region").alias("geo_regions_seen"),
 F.first("device_type").alias("primary_device"),
 F.max("event_timestamp").alias("last_seen_at"),
 )
 .withColumn("engagement_score", # simple weighted score replace with ML feature
 F.round(
 F.col("daily_clicks") * 2.0 +
 F.col("daily_conversions") * 10.0 +
 F.col("daily_impressions") * 0.1,
 2))
 )
