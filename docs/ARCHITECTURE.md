# Architecture Deep-Dive

## Overview

This document explains the technical design decisions in the Acme Media ad events streaming pipeline. Each decision maps to a real trade-off you would face (and be asked about) as an Engineering Manager for a data platform team.

---

## 1. Why Lakeflow SDP over raw Structured Streaming?

Databricks now recommends Lakeflow Spark Declarative Pipelines (SDP) for all new streaming and ETL workloads. The key advantages over writing raw `spark.readStream` jobs:

| Concern | Raw Structured Streaming | Lakeflow SDP |
|---|---|---|
| Orchestration | Manual trigger management | Automatic, dependency-aware |
| Retries | Write your own retry logic | Auto-retries at task flow pipeline level |
| Data quality | Ad-hoc assertions | First-class `@dlt.expect` contracts |
| Lineage | None out of the box | Built-in pipeline graph + event log |
| Checkpointing | Manual checkpoint paths | Managed automatically |
| Cost | Fixed cluster sizing | Enhanced autoscale with Photon |

**EM talking point**: "We moved to Lakeflow SDP because it shifted quality contracts into version-controlled code and reduced the on-call burden from manual checkpoint management."

---

## 2. Medallion Architecture Rationale

### Why three layers?

```
Bronze: Trust nothing, store everything
Silver: Trust the data, filter aggressively 
Gold: Trust the business logic, serve at speed
```

**Bronze** exists because raw events are valuable for reprocessing. If a bug is discovered in Silver logic (wrong dedup window, bad consent filter), you can replay from Bronze without going back to the source.

**Silver** is where data quality is enforced. By centralizing consent filtering and deduplication here, every Gold consumer inherits correctness you don't need each team to reimplement the same checks.

**Gold** is purpose-built for consumers. `campaign_performance_hourly` is optimized for a BI tool querying by campaign and time window. `consumer_daily_signals` is optimized for ML training jobs that need one row per consumer per day. Different grains, different Z-order keys, different refresh cadences.

---

## 3. Deduplication Design

### The problem
Ad event producers (mobile SDKs, web pixels, server-side trackers) all implement at-least-once delivery. In practice, 38% of click events arrive as duplicates from retry storms, network timeouts, or SDK bugs.

### The solution: watermark + dropDuplicates

```python
.withWatermark("event_timestamp", "10 minutes")
.dropDuplicates(["event_id"])
```

**How it works**: Spark maintains a state store of seen `event_id` values within the watermark window. After the watermark passes, state is released the system stops tracking old IDs to bound memory usage.

**Trade-off**: Events arriving more than 10 minutes late will not be deduplicated (they'll be dropped by the watermark). This is acceptable for Acme Media's use case because:
1. Late click events > 10 minutes after the impression have very low attribution value
2. The Gold ML training job uses daily aggregates, so a few missed late events don't materially affect model quality

**EM talking point**: "We set the watermark at 10 minutes based on our p99 producer retry latency. We reviewed our Kafka consumer lag metrics and found that 99.7% of events arrived within 4 minutes. 10 minutes gave us buffer without bloating state store memory."

---

## 4. Consent Filtering (CCPA / GDPR)

```python
@dlt.expect_or_drop("consent_required", "consent_flag = true")
```

Non-consented events are dropped at Silver they never reach Gold or any downstream consumer. This is a hard architectural boundary.

**Why DLT expectations instead of a WHERE filter?**
- Expectations log dropped records to the pipeline event log, giving you an audit trail
- You can query how many records were dropped per run and alert if the non-consent rate spikes (could indicate a consent banner bug)
- Version-controlled quality contracts are easier to audit for compliance reviews

---

## 5. Two Gold Tables, Two Latency Tiers

### `gold_campaign_performance_hourly` Near-real-time
- **Materialized view** refreshes whenever Silver has new data
- Grain: (campaign, advertiser, publisher, geo, device, hour)
- Serves dashboards queried by account managers every few minutes
- Z-ordered on `campaign_id, event_hour` the two most common filter/sort columns

### `gold_consumer_daily_signals` Daily batch
- **Materialized view** refreshed once daily (or triggered by Lakeflow Jobs)
- Grain: (consumer_id, event_date)
- Feeds the ML training pipeline that produces Acme Media's audience propensity scores
- Z-ordered on `consumer_id, event_date`

**Why not just one Gold table?** Different consumers have different needs:
- Dashboard: needs hourly resolution, filters by campaign, doesn't care about consumer_id
- ML: needs daily resolution, filters by consumer_id, doesn't care about hour

Two tables, each with the right grain and indexing, makes both use cases fast and cheap.

---

## 6. Auto Loader Schema Evolution

```python
.option("cloudFiles.schemaEvolutionMode", "addNewColumns")
```

Over time, upstream producers add new fields to events (new device types, new bid attributes, new geo fields). With `addNewColumns`, Auto Loader automatically widens the Bronze schema without pipeline failure.

**Trade-off**: New columns arrive as `null` in historical records. Downstream Silver and Gold transformations use `coalesce` to handle this gracefully. The alternative `failOnNewColumns` would require a manual schema migration on every upstream change, creating operational toil.

---

## 7. Cost Optimization Decisions

### Photon
Enabled in pipeline config. Photon's vectorized execution engine accelerates the columnar aggregations in Gold (GROUP BY campaign, SUM revenue, COUNT DISTINCT consumers). For high-cardinality aggregates over hundreds of millions of events, Photon typically delivers 25x speedup.

### Autoscale (Enhanced)
```json
"autoscale": { "min_workers": 2, "max_workers": 8, "mode": "ENHANCED" }
```
Enhanced autoscale uses real-time backlog metrics (not just CPU) to scale. During off-peak hours, the cluster drops to 2 workers. During peak ingestion (post-lunch, evening), it scales to 8.

### Auto Optimize + Auto Compact
Prevents small file accumulation in Delta tables. Auto Loader writes small files frequently; without compaction, query scan performance degrades over time as the Parquet file list grows.

### Partition Strategy
Gold tables are not explicitly partitioned they rely on Z-ordering and Delta's data skipping. For tables < 1TB, this outperforms Hive-style date partitioning because:
- Z-ordering works across multiple columns simultaneously
- Delta's min/max statistics skip irrelevant files without full partition scans
- No partition explosion from high-cardinality columns (campaign_id device_type geo)

---

## 8. Failure Modes & Recovery

| Failure | Behavior | Recovery |
|---|---|---|
| Auto Loader file read failure | Task-level retry (3x), then flow retry | Idempotent file re-processed on restart |
| Silver dedup state store corruption | Pipeline restarts from last checkpoint | State rebuilt from Bronze within watermark window |
| Gold aggregation failure | Flow-level retry | Materialized view recomputed from Silver |
| Kafka producer outage | Auto Loader waits; no data loss | Resumes when files resume landing |
| Schema evolution (new field) | `addNewColumns` widens Bronze schema | Silver handles null via coalesce |

**Key principle**: Every layer is replayable from the layer below. Bronze can be reprocessed from the source. Silver can be rebuilt from Bronze. Gold can be rebuilt from Silver.

---

## 9. Monitoring & Alerting Recommendations

For a production deployment, wire these to PagerDuty / Slack:

1. **Pipeline event log** alert if `dropped_records > threshold` for any Silver expectation
2. **Kafka consumer lag** alert if Bronze ingestion falls behind by > 5 minutes
3. **Gold refresh latency** alert if `gold_campaign_performance_hourly` hasn't refreshed in > 30 min
4. **Non-consent rate** alert if > 5% of Bronze events are non-consented (upstream bug signal)
5. **Dedup rate** alert if > 10% of Silver events are duplicates (producer retry storm signal)

Databricks provides built-in pipeline observability through the event log and pipeline graph UI no external tooling needed to get started.
