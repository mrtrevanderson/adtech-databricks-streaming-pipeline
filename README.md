# Acme Media - Post-Transaction Ad Targeting Pipeline

A hands-on Databricks tutorial pipeline that simulates a post-transaction ad targeting
platform built on Spark Structured Streaming and Lakeflow Declarative Pipelines.

The business scenario: a user completes a purchase on an e-commerce website. Within
seconds, the pipeline identifies who they are, what they just bought, and what ad to
serve them next -- personalized based on their profile attributes and purchase context.

---

## Business Context

Post-transaction ad serving is a high-value moment in commerce media. When a user
completes a purchase, they are highly engaged and receptive to relevant offers.
The challenge is speed and personalization: you need to know who they are and what
to serve them before they leave the confirmation page.

This pipeline solves that by:
1. Streaming every user interaction on the e-commerce site in real time
2. Identifying the user and matching them to their first-party profile via a stream-static join
3. Detecting the purchase event and generating a targeting record immediately
4. Maintaining a rolling behavioral profile for ML model refreshes

---

## Streaming Techniques Demonstrated

This pipeline is designed as a learning resource. Each Silver layer table demonstrates
a distinct Spark Structured Streaming technique:

| Technique | Where | What it teaches |
|---|---|---|
| Auto Loader (CSV) | Bronze | Incremental file ingestion with schema declaration |
| Watermarks | silver_ecommerce_events | Tolerating late-arriving mobile events |
| AUTO CDC | silver_user_profiles | Merging INSERT/UPDATE/DELETE into a current-state table |
| Stream-Static Join | silver_enriched_purchases | Streaming events joined to a batch profile snapshot |
| Stateful Session Aggregation | silver_session_summary | Building behavioral session windows |

---

## Architecture

```
E-Commerce Website
    |                               |
CSV: ecommerce_events        CSV: user_profiles
(clickstream)                (identity/attributes + CDC ops)
    |                               |
    v                               v
[Auto Loader]                 [Auto Loader]
    |                               |
    v                               v
bronze_ecommerce_events     bronze_user_profiles_raw
(raw, append-only)          (raw CDC records preserved)
    |                               |
    v                               v
silver_ecommerce_events     silver_user_profiles
(watermark + consent filter)  (AUTO CDC - SCD Type 1)
    |           |                   |
    |           +-------------------+
    |           | Stream-Static Join
    |           v
    |    silver_enriched_purchases
    |    (purchase + profile merged)
    |
    v
silver_session_summary
(stateful session window agg)
    |                           |
    v                           v
gold_post_transaction_triggers  gold_user_targeting_profile
(ad serve trigger per purchase) (aggregate behavioral profile)
    |                           |
    v                           v
Ad Serving Engine           ML Audience Model
```

See `diagrams/pipeline_architecture.mermaid` for the full visual.

---

## Repo Structure

```
adtech-databricks-streaming-pipeline/
├── pipeline/
│   ├── 01_bronze_ingestion.sql      # Auto Loader ingestion via read_files() SQL function (no Python needed)
│   ├── 02_silver_transforms.sql      # Watermarks, AUTO CDC, stream-stream join, session agg
│   ├── 03_gold_ad_targeting.sql      # Post-transaction triggers + user targeting profiles
│   └── pipeline_config.json          # Databricks Lakeflow pipeline configuration
├── notebooks/
│   ├── 01_data_generator.py          # Synthetic e-commerce event + profile data generator (batch mode)
│   ├── 02_validation_monitoring.py   # Layer-by-layer validation, monitoring, and dupe rate queries
│   ├── 03_cleanup.sql                # Drop all pipeline tables (use before Full Refresh or schema changes)
│   └── 04_continuous_data_generator.py  # Continuous data generator with configurable interval for testing continuous mode
├── sample_data/
│   ├── ecommerce_events_sample.csv   # Sample clickstream events (reference schema)
│   └── user_profiles_sample.csv      # Sample user profiles with CDC operations
├── diagrams/
│   └── pipeline_architecture.mermaid # Architecture diagram
└── README.md
```

---

## Prerequisites

| Requirement | Notes |
|---|---|
| Databricks workspace | AWS, Azure, or GCP |
| Unity Catalog enabled | Required for catalog.schema.table paths |
| Lakeflow SDP (DLT) Advanced edition | Required for AUTO CDC |
| `ius_unity_prod` catalog created | Or update catalog references in all files |

---

## Setup & Run

### 1. Create catalog, schema, and volumes

```sql
CREATE CATALOG IF NOT EXISTS ius_unity_prod;
CREATE SCHEMA  IF NOT EXISTS ius_unity_prod.sandbox;
CREATE SCHEMA  IF NOT EXISTS ius_unity_prod.sandbox;

CREATE VOLUME IF NOT EXISTS ius_unity_prod.sandbox.ecommerce_events;
CREATE VOLUME IF NOT EXISTS ius_unity_prod.sandbox.user_profiles;
CREATE VOLUME IF NOT EXISTS ius_unity_prod.sandbox._schema_hints;
```

### 2. Import repo into Databricks

Workspace -> Repos -> Add Repo -> paste your GitHub URL

### 3. Generate sample data

**Batch mode (triggered pipeline):**
Open `notebooks/01_data_generator.py` and run all cells.
This writes 10 batches of synthetic CSV events and profile updates to the volumes.

**Continuous mode:**
Open `notebooks/04_continuous_data_generator.py`. Set `BATCH_INTERVAL_S` (default: 10 seconds)
and `MAX_BATCHES` (0 = run forever). Run alongside the pipeline to observe real-time ingestion.
Interrupt the kernel to stop.

### 4. Create the pipeline

Option A - UI:
1. Workflows -> Pipelines -> Create Pipeline
2. Name: `acme_ad_events_pipeline`
3. Add source files in order: 01_bronze_ingestion.sql, 02_silver_transforms.sql, 03_gold_ad_targeting.sql
4. Set catalog: `ius_unity_prod`, target schema: `sandbox`
5. Click Start

Option B - config file:
Upload `pipeline/pipeline_config.json` via the Pipelines API or CLI.

### 5. Validate results

Open `notebooks/02_validation_monitoring.py` and run each cell to verify:
- Row count funnel across all 8 tables
- Zero data quality violations in Silver
- AUTO CDC merged profiles correctly (one row per user)
- Stream-stream join populated enriched purchases
- Gold targeting records have bid prices and ad recommendations

---

## Key Design Decisions

**Why is the entire pipeline in SQL?**
All three layers (Bronze, Silver, Gold) use Lakeflow SQL. Bronze uses the `read_files()`
function with `STREAM` keyword -- the SQL equivalent of Auto Loader, no Python required.
This keeps the entire pipeline in a single language, making it easier to read, maintain,
and hand off to analysts or data engineers who may not be comfortable with PySpark.

**Why a stream-static join instead of a stream-stream join?**
`silver_user_profiles` is built via AUTO CDC (`APPLY CHANGES INTO`) which writes MERGE
commits to the underlying Delta table. Spark Structured Streaming rejects non-append
sources, so streaming from `silver_user_profiles` fails. The fix is a stream-static join:
stream `silver_ecommerce_events` (append-only), batch-lookup `silver_user_profiles`.
Spark reads the latest profile snapshot each microbatch so loyalty tier, LTV, and
interests always reflect the most recent values without watermark overhead on the profile side.

**Why SCD Type 1 for user profiles?**
The ad targeting use case needs the current state of a user's profile, not history.
SCD Type 1 (overwrite on update) keeps the table small and query-fast.
If profile history is needed (for model training audits), add SCD Type 2 as a separate table.

**Why is gold_post_transaction_triggers a Streaming Table and gold_user_targeting_profile a Materialized View?**
`gold_post_transaction_triggers` needs low latency — offers must be available within seconds
of purchase for ad serving. It contains no aggregations (just CASE-based SELECT), so
append-only streaming mode works fine. `gold_user_targeting_profile` uses COUNT(DISTINCT)
and COLLECT_SET which require update/complete output mode — not supported in streaming.
Materialized View runs as a batch query with no output mode restriction.

**Measured pipeline latency:**
In continuous mode, `ingest_to_gold_sec` (Auto Loader pickup → gold write) is consistently
~11 seconds. For sub-second ad serving, the gold table feeds a low-latency key-value store
(e.g. Redis) keyed by `order_id` with a 30-minute TTL. The ad server does a Redis lookup
in <1ms rather than querying the Delta table directly.

**State management across the pipeline:**

Each table in the pipeline has a different memory and state profile:

| Table | Stateful? | Why |
|---|---|---|
| bronze_ecommerce_events | ❌ No | Pure append via Auto Loader — read file, write rows, no state |
| bronze_user_profiles_raw | ❌ No | Same — raw CDC records appended as-is |
| silver_ecommerce_events | ✅ Yes | Watermark holds late-arriving events in state for up to 15 minutes |
| silver_user_profiles | ✅ Yes | AUTO CDC (APPLY CHANGES INTO) maintains merge state internally |
| silver_enriched_purchases | ⚠️ Partial | Watermark on event side only — profile side is a stateless batch snapshot read |
| silver_session_summary | ❌ No | Materialized View — runs as a batch query, no streaming state |
| gold_post_transaction_triggers | ❌ No | Pure streaming SELECT with CASE logic — no joins, no aggregations |
| gold_user_targeting_profile | ❌ No | Materialized View — batch query, no streaming state |

**Bronze memory profile:**
Bronze has zero stateful operators — no joins, no aggregations, no watermarks. Memory
concerns for Bronze are operational rather than computational:
- **Small file accumulation** — one CSV ingested every 10 seconds produces thousands of
  tiny Delta files over time, degrading read performance. Mitigate with periodic `OPTIMIZE`.
- **Auto Loader checkpoint growth** — file tracking metadata grows as files accumulate.
  Manageable but monitor Volume size in long-running pipelines.
- **Unbounded table growth** — Bronze keeps all raw rows including dupes. Set Delta
  retention policies and run `VACUUM` to reclaim storage:

```sql
ALTER TABLE ius_unity_prod.sandbox.bronze_ecommerce_events
SET TBLPROPERTIES (
  'delta.logRetentionDuration'        = 'interval 7 days',
  'delta.deletedFileRetentionDuration' = 'interval 7 days'
);
OPTIMIZE ius_unity_prod.sandbox.bronze_ecommerce_events;
VACUUM  ius_unity_prod.sandbox.bronze_ecommerce_events;
```

**Where actual memory pressure lives:**
The real state burden is in Silver — the 15-minute watermark on `silver_ecommerce_events`
buffers unmatched events in executor memory, and the stream-static join reads the full
`silver_user_profiles` snapshot every microbatch. At 200 synthetic users this is trivial.
At ACME's scale of 200M profiles, the static side would need to be partitioned by
`user_id` hash or moved to a feature store to avoid full table scans per microbatch.

**Why session windows instead of tumbling time windows?**
A tumbling 1-hour window would split a single user session across window boundaries.
Session-based aggregation (GROUP BY session_id) correctly captures the complete funnel
for each visit, regardless of when it started or how long it lasted.

---

## Cost Optimization Considerations

The following optimizations are not implemented in this demo pipeline but should be
considered before moving to production at scale.

### Compute

**Triggered mode for non-real-time tables**
`gold_user_targeting_profile` and `silver_session_summary` are Materialized Views used
for ML and analytics — not real-time ad serving. Running them on a schedule (e.g. hourly)
instead of continuously avoids keeping a cluster alive for tables that don't need
sub-second freshness. Only `gold_post_transaction_triggers` and its upstream streaming
tables need to run continuously.

**Right-size the cluster**
Streaming pipelines don't need large workers. A single-node or 2-worker cluster handles
low-to-medium event volume. Enable auto-scaling with a conservative max to absorb traffic
spikes without over-provisioning at baseline.

**Spot / preemptible instances**
Streaming workloads tolerate interruptions if checkpoints are healthy. AWS Spot instances
are 60-90% cheaper than on-demand. Lakeflow checkpoints to S3 so a spot interruption
triggers a clean resume from the last committed offset, not a full reprocess.

### Storage

**OPTIMIZE + VACUUM on Bronze**
The data generator writes one CSV file every 10 seconds. Over hours this produces
thousands of tiny Delta files, increasing S3 GET costs and degrading read performance.
Run `OPTIMIZE` to compact files and `VACUUM` to remove obsolete Delta versions:

```sql
OPTIMIZE ius_unity_prod.sandbox.bronze_ecommerce_events;
VACUUM   ius_unity_prod.sandbox.bronze_ecommerce_events RETAIN 168 HOURS;

OPTIMIZE ius_unity_prod.sandbox.bronze_user_profiles_raw;
VACUUM   ius_unity_prod.sandbox.bronze_user_profiles_raw RETAIN 168 HOURS;
```

**Delta retention policies**
Bronze retains all historical file versions by default. 7-day retention is sufficient
for most pipelines and significantly reduces S3 storage costs over time:

```sql
ALTER TABLE ius_unity_prod.sandbox.bronze_ecommerce_events
SET TBLPROPERTIES (
  'delta.logRetentionDuration'         = 'interval 7 days',
  'delta.deletedFileRetentionDuration' = 'interval 7 days'
);
```

**Partition pruning on Gold**
Add `event_date` as a partition column on `gold_post_transaction_triggers` so downstream
ad server queries and analytics only scan the relevant day's data rather than the full table.

### Pipeline Architecture

**Tighten the watermark**
The 15-minute watermark holds 15 minutes of events in executor memory, requiring a larger
cluster to avoid OOM. If telemetry shows p99 late arrival is actually 3-5 minutes, tighten
the watermark to reduce state size and run a smaller cluster. Instrument this with the
dead letter table pattern to measure actual late arrival distribution before tuning.

**Minimum microbatch interval**
In continuous mode Spark processes microbatches as fast as possible. Setting a minimum
trigger interval (e.g. 30 seconds) reduces the number of full `silver_user_profiles`
snapshot reads per hour on the stream-static join side, directly reducing compute cost
with minimal latency impact for most ad serving SLAs.

**Filter at Bronze**
The consent filter (`consent_flag = true`) currently runs in Silver, meaning non-consented
rows flow through Bronze ingestion and consume Bronze compute. Moving the filter to Bronze
would reduce the data volume Silver processes by ~3%, compounding savings across all
downstream layers.

**Dead letter table for late events**
Late events dropped by the watermark are currently silently discarded. Routing them to a
dead letter table enables watermark tuning based on real data rather than conservative
estimates, potentially allowing a tighter watermark and smaller cluster over time.

### Scale Considerations (200M Profiles)

At ACME's scale the stream-static join against `silver_user_profiles` becomes the
dominant cost driver. Every microbatch reads the full profile snapshot — at 200M rows
this is a full table scan per batch. Mitigations:

- **Partition by user_id hash** — each executor only loads the profile slice it needs
- **Databricks Feature Store** — profiles live in a low-latency feature store, the join
  becomes a lightweight lookup rather than a full Delta scan
- **Redis serving layer** — pre-computed offers written to Redis keyed by `order_id`
  with a 30-minute TTL, so the ad server never queries Delta directly at serve time

A well-tuned triggered pipeline vs a naive continuous one is typically **3-5x cheaper**
for a workload of this type at production scale.
