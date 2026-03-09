"""
LAYER 1: BRONZE - Auto Loader Ingestion (Python)
=================================================
Bronze is implemented in Python because Auto Loader (cloudFiles) is not
available in SQL. All other layers are in SQL (see 02_silver_transforms.sql
and 03_gold_ad_targeting.sql).

Two sources are ingested:
  1. ecommerce_events  -- CSV clickstream from e-commerce website
  2. user_profiles     -- CSV consumer profile records (used for CDC in Silver)
"""

import dlt
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType, StructField,
    StringType, DoubleType, IntegerType, TimestampType, BooleanType
)

# ---------------------------------------------------------------------------
# CONFIG
# ---------------------------------------------------------------------------
EVENTS_PATH         = "/Volumes/acme_catalog/raw/ecommerce_events/"
PROFILES_PATH       = "/Volumes/acme_catalog/raw/user_profiles/"
EVENTS_SCHEMA_PATH  = "/Volumes/acme_catalog/raw/_schema_hints/events/"
PROFILES_SCHEMA_PATH= "/Volumes/acme_catalog/raw/_schema_hints/profiles/"

# ---------------------------------------------------------------------------
# SCHEMAS
# ---------------------------------------------------------------------------
ecommerce_event_schema = StructType([
    StructField("event_id",          StringType(),    False),
    StructField("event_timestamp",   TimestampType(), False),
    StructField("session_id",        StringType(),    False),
    StructField("user_id",           StringType(),    True),   # null = anonymous user
    StructField("event_type",        StringType(),    False),  # page_view | add_to_cart | checkout_start | purchase
    StructField("page_url",          StringType(),    True),
    StructField("product_id",        StringType(),    True),
    StructField("product_category",  StringType(),    True),
    StructField("product_price",     DoubleType(),    True),
    StructField("quantity",          IntegerType(),   True),
    StructField("order_id",          StringType(),    True),   # only populated on purchase
    StructField("consent_flag",      BooleanType(),   False),  # CCPA/GDPR consent
    StructField("device_type",       StringType(),    True),
    StructField("geo_region",        StringType(),    True),
    StructField("ip_hash",           StringType(),    True),   # PII-safe hashed IP
])

user_profile_schema = StructType([
    StructField("user_id",                  StringType(),  False),
    StructField("email_hash",               StringType(),  True),
    StructField("age_band",                 StringType(),  True),
    StructField("gender",                   StringType(),  True),
    StructField("income_band",              StringType(),  True),
    StructField("interests",                StringType(),  True),   # comma-separated
    StructField("loyalty_tier",             StringType(),  True),   # bronze | silver | gold | platinum
    StructField("lifetime_value_usd",       DoubleType(),  True),
    StructField("preferred_categories",     StringType(),  True),   # comma-separated
    StructField("last_purchase_category",   StringType(),  True),
    StructField("total_orders",             IntegerType(), True),
    StructField("consent_flag",             BooleanType(), False),
    StructField("operation",                StringType(),  False),  # INSERT | UPDATE | DELETE (CDC)
    StructField("updated_at",               TimestampType(),False),
])

# ===========================================================================
# BRONZE 1: E-commerce Clickstream Events
# ===========================================================================
@dlt.table(
    name="bronze_ecommerce_events",
    comment="""
        Raw e-commerce clickstream events ingested via Auto Loader (CSV).
        Captures every user interaction on the e-commerce site:
        page views, add-to-cart, checkout starts, and purchases.
        Append-only, no transformations applied.
    """,
    table_properties={
        "quality": "bronze",
        "pipelines.autoOptimize.managed": "true",
    }
)
def bronze_ecommerce_events():
    """
    TECHNIQUE: Auto Loader with CSV + explicit schema
    Auto Loader monitors the landing path for new CSV files and ingests them
    incrementally with exactly-once guarantees via checkpointing.
    Schema is declared explicitly to avoid inference errors on empty files.
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
# ===========================================================================
@dlt.table(
    name="bronze_user_profiles_raw",
    comment="""
        Raw user profile records ingested via Auto Loader (CSV).
        Each row contains an operation column (INSERT/UPDATE/DELETE)
        representing changes to the consumer identity/attribute store.
        This raw table feeds the AUTO CDC flow in Silver.
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
    The raw bronze table preserves all versions before CDC merge in Silver.
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
