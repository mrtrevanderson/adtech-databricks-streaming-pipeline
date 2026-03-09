"""
Acme Media Ad Events Streaming Pipeline
========================================
Simulates Acme Media's real-time ad event ingestion pipeline using:
  - Spark Structured Streaming (Auto Loader for ingestion)
  - Lakeflow Spark Declarative Pipelines (SDP)
  - Medallion Architecture: Bronze -> Silver -> Gold
  - Delta Lake for ACID storage

Architecture: Kafka/S3 -> Bronze (raw) -> Silver (clean/deduped) -> Gold (aggregated)

Note: Bronze ingestion uses Python + Auto Loader (no SQL equivalent for cloudFiles).
      Silver and Gold are implemented in SQL using the @dlt.view / CREATE STREAMING TABLE
      and CREATE MATERIALIZED VIEW syntax.
"""

import dlt
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType, StructField, StringType,
    DoubleType, TimestampType, BooleanType
)

# ---------------------------------------------------------------------------
# CONFIG
# ---------------------------------------------------------------------------
RAW_EVENTS_PATH   = "/Volumes/acme_catalog/raw/ad_events/"
SCHEMA_HINTS_PATH = "/Volumes/acme_catalog/raw/schema_hints/"

# ---------------------------------------------------------------------------
# SCHEMA
# ---------------------------------------------------------------------------
ad_event_schema = StructType([
    StructField("event_id",        StringType(),    False),  # UUID
    StructField("event_type",      StringType(),    False),  # click | impression | conversion
    StructField("event_timestamp", TimestampType(), False),
    StructField("consumer_id",     StringType(),    True),   # first-party profile ID
    StructField("campaign_id",     StringType(),    False),
    StructField("advertiser_id",   StringType(),    False),
    StructField("publisher_id",    StringType(),    True),
    StructField("placement_id",    StringType(),    True),
    StructField("bid_price_usd",   DoubleType(),    True),   # winning bid (impressions only)
    StructField("revenue_usd",     DoubleType(),    True),   # CPA/CPL revenue (conversions)
    StructField("user_agent",      StringType(),    True),
    StructField("ip_hash",         StringType(),    True),   # PII-safe hashed IP
    StructField("consent_flag",    BooleanType(),   False),  # CCPA/GDPR consent
    StructField("geo_region",      StringType(),    True),   # US state code
    StructField("device_type",     StringType(),    True),   # mobile | desktop | tablet
])

# ===========================================================================
# LAYER 1: BRONZE — Python required here: Auto Loader (cloudFiles) has no SQL equivalent
# ===========================================================================
@dlt.table(
    name="bronze_ad_events",
    comment="Raw ad events from upstream producers via Auto Loader. Append-only, no transforms.",
    table_properties={
        "quality": "bronze",
        "pipelines.autoOptimize.managed": "true",
    }
)
def bronze_ad_events():
    """
    Auto Loader reads JSON files from the landing zone.
    Handles schema inference, evolution, and exactly-once ingestion with checkpointing.
    Python is required here — cloudFiles is not available in SQL.
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
             .withColumn("_ingest_timestamp", F.current_timestamp())
             .withColumn("_source_file",      F.col("_metadata.file_path"))
    )
