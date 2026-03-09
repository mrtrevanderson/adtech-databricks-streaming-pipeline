# Databricks Setup Guide
# Step-by-step walkthrough to run the Acme Media pipeline in your workspace

---

## API Note: DLT is now pyspark.pipelines

If you have used Delta Live Tables (DLT) before, note that the module has changed.

| Old (DLT) | New (Lakeflow SDP) |
|---|---|
| `import dlt` | `from pyspark import pipelines as dp` |
| `@dlt.table(...)` | `@dp.table(...)` for streaming tables |
| `@dlt.table(...)` | `@dp.materialized_view(...)` for materialized views |
| `@dlt.expect_or_drop(...)` | `@dp.expect_or_drop(...)` |
| `dlt.read_stream(...)` | `dp.read_stream(...)` |
| `dlt.read(...)` | `dp.read(...)` |

Old `import dlt` code still works — Databricks maintains backward compatibility.
But `pyspark.pipelines` is the current recommended module.

---

## Prerequisites

Before starting, confirm you have:
- A Databricks workspace (AWS, Azure, or GCP)
- Unity Catalog enabled on your workspace
- Lakeflow Pipelines (SDP) available — requires Advanced or Core edition
- Your GitHub repo URL for this project

---

## Step 1: Create the Catalog, Schemas, and Volumes

Open a SQL worksheet in your Databricks workspace and run the following.
Volumes are the recommended way to land files for Auto Loader in Unity Catalog.

```sql
-- Create catalog
CREATE CATALOG IF NOT EXISTS ius_unity_prod;

-- Create schemas
CREATE SCHEMA IF NOT EXISTS ius_unity_prod.sandbox;
CREATE SCHEMA IF NOT EXISTS ius_unity_prod.sandbox;

-- Create volumes for Auto Loader landing zones
CREATE VOLUME IF NOT EXISTS ius_unity_prod.sandbox.ecommerce_events;
CREATE VOLUME IF NOT EXISTS ius_unity_prod.sandbox.user_profiles;

-- Create volume for Auto Loader schema hints (inference cache)
CREATE VOLUME IF NOT EXISTS ius_unity_prod.sandbox._schema_hints;
```

Verify they were created:
```sql
SHOW VOLUMES IN ius_unity_prod.sandbox;
```

---

## Step 2: Connect Your GitHub Repo to Databricks

1. In the left sidebar, click **Workspace**
2. Click **Repos** in the left sidebar
3. Click **Add Repo** in the top right
4. Paste your GitHub repo URL:
   `https://github.com/mrtrevanderson/adtech-databricks-streaming-pipeline`
5. Click **Create Repo**

Databricks will clone the repo. You should see the full folder structure:
```
adtech-databricks-streaming-pipeline/
  pipeline/
  notebooks/
  sample_data/
  docs/
  diagrams/
```

---

## Step 3: Create a Compute Cluster (for the Data Generator)

You need a cluster to run the data generator notebook.

1. Click **Compute** in the left sidebar
2. Click **Create compute**
3. Configure:
   - **Name:** `acme-dev-cluster`
   - **Policy:** Unrestricted (or your org's standard)
   - **Access mode:** Single user
   - **Databricks Runtime:** 15.4 LTS or later (includes Spark 3.5+)
   - **Node type:** `i3.xlarge` (AWS) or `Standard_DS3_v2` (Azure) — smallest available
   - **Auto terminate:** 30 minutes
4. Click **Create compute**

---

## Step 4: Run the Data Generator

This seeds your Volumes with synthetic CSV files for the pipeline to ingest.

1. Navigate to **Workspace → Repos → adtech-databricks-streaming-pipeline → notebooks**
2. Open `01_data_generator.py`
3. In the top right, attach it to the cluster you created in Step 3
4. Click **Run all**

The notebook will:
- Generate 10 batches of e-commerce event CSV files → `ius_unity_prod.sandbox.ecommerce_events`
- Generate user profile CSV files with INSERT/UPDATE operations → `ius_unity_prod.sandbox.user_profiles`

Verify data landed:
```python
display(spark.read.csv("/Volumes/ius_unity_prod/sandbox/ecommerce_events/", header=True))
```

---

## Step 5: Create the Lakeflow Pipeline

1. In the left sidebar, click **Workflows**
2. Click the **Pipelines** tab
3. Click **Create pipeline**
4. Fill in the settings:

**General**
- **Pipeline name:** `acme_ad_events_pipeline`
- **Pipeline mode:** Triggered (runs on demand — good for development)

**Source code** — add all three files in order:
- Click **Add source code**
- Browse to: `Repos/adtech-databricks-streaming-pipeline/pipeline/01_bronze_ingestion.py`
- Click **Add source code** again
- Browse to: `Repos/adtech-databricks-streaming-pipeline/pipeline/02_silver_transforms.sql`
- Click **Add source code** again
- Browse to: `Repos/adtech-databricks-streaming-pipeline/pipeline/03_gold_ad_targeting.sql`

**Destination**
- **Catalog:** `ius_unity_prod`
- **Target schema:** `sandbox`

**Compute**
- Leave as default (Databricks manages pipeline compute automatically)
- Or enable **Serverless** if available in your workspace region

**Advanced (optional)**
- Add configuration key: `pipelines.enableTrackHistory` = `true`

5. Click **Create**

---

## Step 6: Run the Pipeline

1. On the pipeline detail page, click **Start**
2. Databricks will:
   - Provision compute
   - Analyze dependencies across all three source files
   - Build the execution graph (Bronze → Silver → Gold)
   - Run each flow in the correct order

3. Watch the **Pipeline graph** — each table node turns green as it completes

Expected execution order:
```
bronze_ecommerce_events  ──┐
                            ├──> silver_ecommerce_events ──┬──> silver_enriched_purchases ──> gold_post_transaction_triggers
bronze_user_profiles_raw ──┘                               │
        │                                                  └──> silver_session_summary ──> gold_user_targeting_profile
        └──> silver_user_profiles (AUTO CDC) ──────────────┘
```

A full run on the sample data takes approximately 3-5 minutes.

---

## Step 7: Validate the Results

1. Navigate to **Workspace → Repos → adtech-databricks-streaming-pipeline → notebooks**
2. Open `02_validation_monitoring.py`
3. Attach to your cluster and click **Run all**

What to look for:
- **Row count audit** — all 8 tables should have rows; counts should decrease Bronze → Silver → Gold (expected due to consent filtering and dedup)
- **DQ violations** — all checks should return 0
- **Profile CDC** — `total_profiles` should equal `unique_users` (one row per user after AUTO CDC merge)
- **Enriched purchases** — should show profile attributes (loyalty_tier, age_band) attached to purchase events
- **Ad targeting** — platinum/gold users should have higher `suggested_max_bid_usd`

---

## Step 8: Inspect the Pipeline Graph and Event Log

In the pipeline UI:
- Click any table node to see row counts, duration, and data quality metrics
- Click **Event log** tab to see expectation violation details (rows dropped in Silver)
- Click **Data tab** on a table node to preview the data inline

To query the event log directly:
```sql
SELECT
    timestamp,
    event_type,
    details:flow_name       AS flow_name,
    details:num_output_rows AS output_rows,
    details:dropped_records AS dropped_records
FROM ius_unity_prod.sandbox.sandbox_events
WHERE event_type = 'flow_progress'
ORDER BY timestamp DESC
LIMIT 50;
```

---

## Step 9: Re-run with More Data (Simulate Streaming)

To simulate continuous streaming:
1. Go back to `01_data_generator.py`
2. Change `NUM_BATCHES = 20` at the top
3. Run all cells again — this drops more CSV files into the Volumes
4. Go back to the pipeline and click **Start** again
5. Databricks detects only the new files (Auto Loader tracks what it has already ingested) and processes the delta

This demonstrates the incremental nature of Auto Loader and SDP — only new data is processed on each run.

---

## Troubleshooting

| Problem | Likely cause | Fix |
|---|---|---|
| Pipeline fails on bronze tables | Volumes don't exist | Re-run Step 1 SQL |
| Schema mismatch error | CSV header doesn't match schema | Check sample_data/ CSVs match the schema in 01_bronze_ingestion.py |
| AUTO CDC fails | SDP Advanced edition required | Check workspace edition under Admin Settings |
| No rows in Silver | Consent filter dropping everything | Check consent_flag column in your CSV — must be `true` (lowercase string) |
| Stream-stream join empty | No matching user_ids between events and profiles | Run the data generator for both sources before starting pipeline |
| `dp module not found` | Old runtime | Upgrade to Databricks Runtime 15.4 LTS or later |
