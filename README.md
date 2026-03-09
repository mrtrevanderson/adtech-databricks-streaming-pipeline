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
2. Identifying the user and matching them to their first-party profile via a stream-stream join
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
| Stream-Stream Join | silver_enriched_purchases | Enriching events with profile data in real time |
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
    |           | Stream-Stream Join
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
│   ├── 01_bronze_ingestion.py        # Auto Loader ingestion (Python - required for cloudFiles)
│   ├── 02_silver_transforms.sql      # Watermarks, AUTO CDC, stream-stream join, session agg
│   ├── 03_gold_ad_targeting.sql      # Post-transaction triggers + user targeting profiles
│   └── pipeline_config.json          # Databricks Lakeflow pipeline configuration
├── notebooks/
│   ├── 01_data_generator.py          # Synthetic e-commerce event + profile data generator
│   └── 02_validation_monitoring.py   # Layer-by-layer validation and monitoring queries
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
| `acme_catalog` catalog created | Or update catalog references in all files |

---

## Setup & Run

### 1. Create catalog, schema, and volumes

```sql
CREATE CATALOG IF NOT EXISTS acme_catalog;
CREATE SCHEMA  IF NOT EXISTS acme_catalog.raw;
CREATE SCHEMA  IF NOT EXISTS acme_catalog.acme_ad_pipeline;

CREATE VOLUME IF NOT EXISTS acme_catalog.raw.ecommerce_events;
CREATE VOLUME IF NOT EXISTS acme_catalog.raw.user_profiles;
CREATE VOLUME IF NOT EXISTS acme_catalog.raw._schema_hints;
```

### 2. Import repo into Databricks

Workspace -> Repos -> Add Repo -> paste your GitHub URL

### 3. Generate sample data

Open `notebooks/01_data_generator.py` and run all cells.
This writes 10 batches of synthetic CSV events and profile updates to the volumes.

### 4. Create the pipeline

Option A - UI:
1. Workflows -> Pipelines -> Create Pipeline
2. Name: `acme_ad_events_pipeline`
3. Add source files in order: 01_bronze_ingestion.py, 02_silver_transforms.sql, 03_gold_ad_targeting.sql
4. Set catalog: `acme_catalog`, target schema: `acme_ad_pipeline`
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

**Why is Bronze Python and Silver/Gold SQL?**
Auto Loader's `cloudFiles` format is only available via the Python/Scala DataFrame API.
Everything else is expressed in SQL to keep transformations readable and easy to maintain.

**Why a stream-stream join instead of a stream-static join?**
User profiles update in real time (new purchases, consent changes, loyalty tier upgrades).
A stream-static join would miss profile updates that arrive after the pipeline starts.
The stream-stream join ensures every purchase event is enriched with the most current profile.

**Why SCD Type 1 for user profiles?**
The ad targeting use case needs the current state of a user's profile, not history.
SCD Type 1 (overwrite on update) keeps the table small and query-fast.
If profile history is needed (for model training audits), add SCD Type 2 as a separate table.

**Why session windows instead of tumbling time windows?**
A tumbling 1-hour window would split a single user session across window boundaries.
Session-based aggregation (GROUP BY session_id) correctly captures the complete funnel
for each visit, regardless of when it started or how long it lasted.
