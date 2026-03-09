# Acme Media Ad Events Streaming Pipeline

A hands-on tutorial pipeline modeled on **Acme Media's** real-time ad event infrastructure. 
Built with **Spark Structured Streaming** and **Lakeflow Spark Declarative Pipelines (SDP)** on Databricks.

> **Purpose:** Interview preparation / learning project. 
> Demonstrates the medallion architecture pattern used in adtech data platforms processing click, impression, and conversion events at scale.

---

## Business Context

[Acme Media](https://acmemedia.com) is a commerce media company with 200M+ first-party consumer profiles. Their platform:

- Ingests ad click, impression, and conversion events in real time
- Deduplicates and consent-filters events before any downstream use
- Feeds live campaign dashboards for advertiser reporting
- Produces daily consumer engagement signals for ML audience modeling
- Uses **Databricks** (via official partnership) for data intelligence and Delta Sharing

This pipeline replicates that core hot path.

---

## Architecture

```
Kafka / S3 Landing Zone
 
 
 [Auto Loader] schema inference + evolution, exactly-once
 
 
 
 BRONZE bronze_ad_events Raw, append-only, full fidelity
 
 Spark Structured Streaming
 
 
 SILVER silver_ad_events Deduped Consent-filtered DLT expectations
 
 
 
 
 
 GOLD campaign_perf GOLD consumer_daily_signals
 (hourly) Live dashboard (daily) ML training
 
```

See [`diagrams/pipeline_architecture.svg`](diagrams/pipeline_architecture.svg) for the full visual.

---

## Pipeline Layers

### Bronze `bronze_ad_events`
- Reads JSON files via **Auto Loader** (`cloudFiles` format)
- Handles schema inference, evolution, and new column addition automatically
- Append-only; no transformations applied
- Captures `_ingest_timestamp` and `_source_file` for lineage

### Silver `silver_ad_events`
- **Deduplication** on `event_id` using Structured Streaming watermarks (10-minute late tolerance)
- **Consent filtering** drops non-consented events (CCPA/GDPR compliance)
- **DLT Expectations** enforce data quality; rows failing checks are dropped and logged:
 - `valid_event_type` only click / impression / conversion
 - `consent_required` consent_flag must be true
 - `non_null_event_id` UUID must be present
 - `non_null_campaign` campaign_id must be present
- Derives `event_date`, `event_hour`, `is_mobile`
- Change Data Feed (CDF) enabled for downstream CDC consumers

### Gold `gold_campaign_performance_hourly`
- Hourly rollup at `(campaign_id, advertiser_id, publisher_id, geo_region, device_type)` grain
- Computes: impressions, clicks, conversions, CTR, CVR, eCPM, revenue, unique consumers
- **Primary feed for live campaign performance dashboards**
- Z-ordered on `campaign_id, event_hour` for fast dashboard queries

### Gold `gold_consumer_daily_signals`
- Daily rollup at `(consumer_id, event_date)` grain
- Computes: engagement_score (weighted), advertisers interacted, daily revenue
- **Feeds nightly ML training job** that refreshes Acme Media's audience targeting models

---

## Repo Structure

```
acme-streaming-pipeline/
 pipeline/
 acme_ad_events_pipeline.py # Main Lakeflow SDP pipeline (BronzeSilverGold)
 pipeline_config.json # Databricks pipeline configuration
 notebooks/
 01_data_generator.py # Synthetic event data generator
 02_validation_monitoring.py # Data quality checks + monitoring queries
 diagrams/
 pipeline_architecture.svg # Architecture diagram
 docs/
 ARCHITECTURE.md # Deep-dive technical documentation
 README.md
```

---

## Prerequisites

| Requirement | Notes |
|---|---|
| Databricks workspace | AWS, Azure, or GCP |
| Unity Catalog enabled | Required for catalog.schema.table paths |
| Serverless compute (optional) | Or standard compute with DBR 13.3 LTS+ |
| Lakeflow SDP (DLT) enabled | Advanced or Core edition |
| `acme_catalog` catalog created | Or update catalog name in config |

---

## Setup & Run

### 1. Create the catalog and volume

```sql
CREATE CATALOG IF NOT EXISTS acme_catalog;
CREATE SCHEMA IF NOT EXISTS acme_catalog.raw;
CREATE VOLUME IF NOT EXISTS acme_catalog.raw.ad_events;
CREATE VOLUME IF NOT EXISTS acme_catalog.raw.schema_hints;
```

### 2. Import the repo into Databricks Repos

```
Workspace Repos Add Repo paste your GitHub URL
```

### 3. Generate synthetic data

Open `notebooks/01_data_generator.py` in your workspace and run all cells. 
This writes 10 batches of synthetic JSON events to the Auto Loader source path.

### 4. Create the pipeline

Option A **UI**:
1. Go to **Workflows Pipelines Create Pipeline**
2. Name it `acme_ad_events_pipeline`
3. Set source file: `/Repos/.../pipeline/acme_ad_events_pipeline.py`
4. Set catalog: `acme_catalog`
5. Click **Start**

Option B **API / CLI**:
```bash
databricks pipelines create --json @pipeline/pipeline_config.json
databricks pipelines start <pipeline-id>
```

### 5. Validate

Open `notebooks/02_validation_monitoring.py` and run each cell to verify:
- Row count funnel (Bronze Silver Gold)
- Zero quality violations in Silver
- Gold aggregates populated correctly

---

## Key Concepts Demonstrated

| Concept | Where |
|---|---|
| Auto Loader (cloudFiles) | `bronze_ad_events()` |
| Watermarks for late data | `silver_ad_events()` `withWatermark("event_timestamp", "10 minutes")` |
| Stateful deduplication | `silver_ad_events()` `dropDuplicates(["event_id"])` |
| DLT data quality expectations | `@dlt.expect_or_drop` decorators on silver |
| Materialized views (batch Gold) | `gold_campaign_performance_hourly()` |
| Streaming tables (append Gold) | `gold_consumer_daily_signals()` |
| Delta Change Data Feed | `"delta.enableChangeDataFeed": "true"` on silver |
| Z-ordering for query perf | `pipelines.autoOptimize.zOrderCols` on gold tables |

---

## Cost Optimization Notes

- **Photon enabled** in `pipeline_config.json` accelerates columnar operations on Gold aggregates
- **Autoscale 28 workers** scales down during off-peak ingestion lulls
- **Auto Optimize + Auto Compact** prevents small file accumulation in Delta tables
- **Tiered storage** cold Bronze files can be transitioned to S3 Infrequent Access after 30 days
- For higher scale: consider **Trigger.AvailableNow** for SilverGold to decouple streaming and batch costs

---

## Interview Talking Points

This pipeline directly maps to the technical areas in a Data Platform EM interview:

- **Streaming vs batch**: BronzeSilver is streaming (low latency dedup/consent); SilverGold campaign perf is a materialized view (batch refresh) because dashboards don't need sub-second freshness
- **Exactly-once semantics**: Auto Loader + Delta checkpointing provides exactly-once; Silver adds application-level dedup with watermarks
- **Late data handling**: Watermark set to 10 minutes; events arriving later are dropped acceptable for ad attribution at this latency tier
- **Cost optimization**: Photon + autoscale + auto-compact; Z-ordering on Gold tables cuts downstream query scan costs
- **Data quality as code**: DLT expectations are version-controlled quality contracts, not ad-hoc checks
- **Acme Media + Databricks**: Acme Media uses Databricks (Delta Sharing partnership) this stack is directly relevant
