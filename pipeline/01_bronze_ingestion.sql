-- =============================================================================
-- LAYER 1: BRONZE - Auto Loader Ingestion (SQL)
-- =============================================================================
-- Uses read_files() with STREAM keyword -- the SQL equivalent of Auto Loader.
-- read_files() leverages cloudFiles under the hood, giving you:
--   - Incremental file ingestion (only new files per run)
--   - Exactly-once guarantees via checkpointing (managed by SDP)
--   - Schema inference and evolution
--   - Support for CSV, JSON, Parquet, Avro, ORC
--
-- SCHEMA APPROACH:
--   Two modes available -- comment/uncomment to switch:
--
--   MODE 1 (current): cloudFiles.schemaHints + schemaEvolutionMode=addNewColumns
--     - Auto Loader infers schema but respects type hints for known columns
--     - New columns in incoming files are automatically added to the table
--     - Good for: testing schema evolution, early-stage pipelines, exploratory work
--
--   MODE 2: explicit schema => '...'
--     - Strict type enforcement, no evolution allowed
--     - Pipeline fails fast if upstream sends unexpected columns or types
--     - Good for: production pipelines with a stable, contracted schema
-- =============================================================================


-- -----------------------------------------------------------------------------
-- BRONZE 1: E-commerce Clickstream Events (CSV)
-- -----------------------------------------------------------------------------
CREATE OR REFRESH STREAMING TABLE bronze_ecommerce_events
  COMMENT "Raw e-commerce clickstream events ingested via Auto Loader SQL (read_files).
           Captures every user interaction: page_view, add_to_cart,
           checkout_start, and purchase. Append-only, no transforms applied."
  TBLPROPERTIES (
    "quality"                         = "bronze",
    "pipelines.autoOptimize.managed"  = "true"
  )
AS
SELECT
    *,
    current_timestamp()             AS _ingest_timestamp,
    _metadata.file_path             AS _source_file
FROM STREAM read_files(
    '/Volumes/ius_unity_prod/sandbox/ecommerce_events/',
    format                        => 'csv',
    header                        => true,
    -- MODE 1: schema hints + evolution (uncomment to use)
    -- cloudFiles.schemaHints     => 'event_id STRING, event_timestamp TIMESTAMP, product_price DOUBLE, quantity INT, consent_flag BOOLEAN',
    -- schemaEvolutionMode        => 'addNewColumns',

    -- MODE 2: explicit schema (strict, no evolution)
    schema => '
        event_id        STRING      NOT NULL,
        event_timestamp TIMESTAMP   NOT NULL,
        session_id      STRING      NOT NULL,
        user_id         STRING,
        event_type      STRING      NOT NULL,
        page_url        STRING,
        product_id      STRING,
        product_category STRING,
        product_price   DOUBLE,
        quantity        INT,
        order_id        STRING,
        consent_flag    BOOLEAN     NOT NULL,
        device_type     STRING,
        geo_region      STRING,
        ip_hash         STRING
    '
);


-- -----------------------------------------------------------------------------
-- BRONZE 2: User Profile Records (CSV with CDC operations)
-- -----------------------------------------------------------------------------
CREATE OR REFRESH STREAMING TABLE bronze_user_profiles_raw
  COMMENT "Raw user profile records ingested via Auto Loader SQL (read_files).
           Each row includes an operation column (INSERT/UPDATE/DELETE)
           representing changes to the consumer identity store.
           All raw CDC rows preserved here before AUTO CDC merge in Silver."
  TBLPROPERTIES (
    "quality"                         = "bronze",
    "pipelines.autoOptimize.managed"  = "true"
  )
AS
SELECT
    *,
    current_timestamp()             AS _ingest_timestamp,
    _metadata.file_path             AS _source_file
FROM STREAM read_files(
    '/Volumes/ius_unity_prod/sandbox/user_profiles/',
    format                        => 'csv',
    header                        => true,
    -- MODE 1: schema hints + evolution (uncomment to use)
    -- cloudFiles.schemaHints     => 'user_id STRING, lifetime_value_usd DOUBLE, total_orders INT, consent_flag BOOLEAN, updated_at TIMESTAMP',
    -- schemaEvolutionMode        => 'addNewColumns',

    -- MODE 2: explicit schema (strict, no evolution)
    schema => '
        user_id                 STRING      NOT NULL,
        email_hash              STRING,
        age_band                STRING,
        gender                  STRING,
        income_band             STRING,
        interests               STRING,
        loyalty_tier            STRING,
        lifetime_value_usd      DOUBLE,
        preferred_categories    STRING,
        last_purchase_category  STRING,
        total_orders            INT,
        consent_flag            BOOLEAN     NOT NULL,
        operation               STRING      NOT NULL,
        updated_at              TIMESTAMP   NOT NULL
    '
);
