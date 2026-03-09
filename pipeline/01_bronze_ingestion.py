"""
LAYER 1: BRONZE - Auto Loader Ingestion (Python)
=================================================
Uses the current Lakeflow SDP API: pyspark.pipelines (replaces the old dlt module).

Key API changes from DLT -> Lakeflow SDP:
  OLD:  import dlt
        @dlt.table(...)
        @dlt.expect_or_drop(...)
        dlt.read_stream(...)

  NEW:  from pyspark import pipelines as dp
        @dp.table(...)                  # streaming tables
        @dp.materialized_view(...)      # materialized views (new decorator)
        @dp.expect_or_drop(...)
        dp.read_stream(...)

Bronze is implemented in Python because Auto Loader (cloudFiles) is not
available in SQL. Silver and Gold are in SQL (02_silver_transforms.sql,
03_gold_ad_targeting.sql).

Two sources ingested:
  1. ecommerce_events      -- CSV clickstream from e-commerce website
  2. user_profiles_raw     -- CSV consumer profile records (feeds AUTO CDC in Silver)
"""

from pyspark import pipelines as dp
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType, StructField,
    StringType, DoubleType, IntegerType, TimestampType, BooleanType
)

# ---------------------------------------------------------------------------
# CONFIG
# ---------------------------------------------------------------------------
EVENTS_PATH          = "/Volumes/acme_catalog/raw/ecommerce_events/"
PROFILES_PATH        = "/Volumes/acme_catalog/raw/user_profiles/"
EVENTS_SCHEMA_PATH   = "/Volumes/acme_catalog/raw/_schema_hints/events/"
PROFILES_SCHEMA_PATH = "/Volumes/acme_catalog/raw/_schema_hints/profiles/"

# ---------------------------------------------------------------------------
# SCHEMAS - declared explicitly for production reliability
# ---------------------------------------------------------------------------
ecommerce_event_schema = StructType([
    StructField("event_id",         StringType(),    False),
    StructField("event_timestamp",  TimestampType(), False),
    StructField("session_id",       StringType(),    False),
    StructField("user_id",          StringType(),    True),   # null = anonymous user
    StructField("event_type",       StringType(),    False),  # page_view | add_to_cart | checkout_start | purchase
    StructField("page_url",         StringType(),    True),
    StructField("product_id",       StringType(),    True),
    StructField("product_category", StringType(),    True),
    StructField("product_price",    DoubleType(),    True),
    StructField("quantity",         IntegerType(),   True),
    StructField("order_id",         StringType(),    True),   # only populated on purchase
    StructField("consent_flag",     BooleanType(),   False),  # CCPA/GDPR consent
    StructField("device_type",      StringType(),    True),
    StructField("geo_region",       StringType(),    True),
    StructField("ip_hash",          StringType(),    True),   # PII-safe hashed IP
])

user_profile_schema = StructType([
    StructField("user_id",                StringType(),    False),
    StructField("email_hash",             StringType(),    True),
    StructField("age_band",               StringType(),    True),
    StructField("gender",                 StringType(),    True),
    StructField("income_band",            StringType(),    True),
    StructField("interests",              StringType(),    True),  # comma-separated
    StructField("loyalty_tier",           StringType(),    True),  # bronze|silver|gold|platinum
    StructField("lifetime_value_usd",     DoubleType(),    True),
    StructField("preferred_categories",   StringType(),    True),  # comma-separated
    StructField("last_purchase_category", StringType(),    True),
    StructField("total_orders",           IntegerType(),   True),
    StructField("consent_flag",           BooleanType(),   False),
    StructField("operation",              StringType(),    False), # INSERT | UPDATE | DELETE
    StructField("updated_at",             TimestampType(), False),
])


# ===========================================================================
# BRONZE 1: E-commerce Clickstream Events
# @dp.table creates a STREAMING TABLE (append-only, incremental ingestion)
# ===========================================================================
@dp.table(
    name="bronze_ecommerce_events",
    comment="""
        Raw e-commerce clickstream events ingested via Auto Loader (CSV).
        Captures every user interaction: page_view, add_to_cart,
        checkout_start, and purchase. Append-only, no transforms applied.
    """,
    table_properties={
        "quality": "bronze",
        "pipelines.autoOptimize.managed": "true",
    }
)
def bronze_ecommerce_events():
    """
    TECHNIQUE: Auto Loader with CSV + explicit schema
    cloudFiles monitors the landing path and ingests new CSV files
    incrementally with exactly-once guarantees via checkpointing.
    schemaEvolutionMode=addNewColumns handles upstream schema changes
    without failing the pipeline.
    """
    return (
        spark.readStream
             .format("cloudFiles")
             .option("cloudFiles.format", "csv")
             .option("cloudFiles.schemaLocation", EVENTS_SCHEMA_PATH)
             .option("header", "true")
             .option("cloudFiles.schemaEvolutionMode", "addNewColumns")
             .schema(ecommerce_event_schema)
             .load(EVENTS_PATH)
             .withColumn("_ingest_timestamp", F.current_timestamp())
             .withColumn("_source_file",      F.col("_metadata.file_path"))
    )


# ===========================================================================
# BRONZE 2: User Profile Records (CDC source)
# @dp.table creates a STREAMING TABLE preserving all raw CDC rows
# ===========================================================================
@dp.table(
    name="bronze_user_profiles_raw",
    comment="""
        Raw user profile records ingested via Auto Loader (CSV).
        Each row has an operation column (INSERT/UPDATE/DELETE) representing
        changes to the consumer identity store.
        Raw CDC rows are preserved here before the AUTO CDC merge in Silver.
    """,
    table_properties={
        "quality": "bronze",
        "pipelines.autoOptimize.managed": "true",
    }
)
def bronze_user_profiles_raw():
    """
    TECHNIQUE: Auto Loader as CDC source
    Profile updates arrive as full-row snapshots with an operation type.
    Bronze preserves all versions; Silver's AUTO CDC merges them into
    a single current-state row per user.
    """
    return (
        spark.readStream
             .format("cloudFiles")
             .option("cloudFiles.format", "csv")
             .option("cloudFiles.schemaLocation", PROFILES_SCHEMA_PATH)
             .option("header", "true")
             .option("cloudFiles.schemaEvolutionMode", "addNewColumns")
             .schema(user_profile_schema)
             .load(PROFILES_PATH)
             .withColumn("_ingest_timestamp", F.current_timestamp())
             .withColumn("_source_file",      F.col("_metadata.file_path"))
    )
