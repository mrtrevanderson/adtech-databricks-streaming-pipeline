# Acme Media Ad Events - Data Generator
# Use this notebook to simulate ad event data landing in the Auto Loader source path.
# Run this in one cluster while the pipeline runs in another to observe real-time ingestion.

import uuid
import random
import time
from datetime import datetime, timezone, timedelta
from pyspark.sql import Row

# ---------------------------------------------------------------------------
# CONFIG - match these to your pipeline's RAW_EVENTS_PATH
# ---------------------------------------------------------------------------
OUTPUT_PATH   = "/Volumes/acme_catalog/raw/ad_events/"
NUM_EVENTS    = 500   # events per batch
NUM_BATCHES   = 10    # how many files to generate
BATCH_DELAY_S = 5     # seconds between files

EVENT_TYPES   = ["impression", "click", "conversion"]
EVENT_WEIGHTS = [0.75, 0.20, 0.05]  # realistic funnel distribution

DEVICE_TYPES = ["mobile", "desktop", "tablet"]
GEO_REGIONS  = ["NY", "CA", "TX", "FL", "IL", "WA", "MA", "GA"]
ADVERTISERS  = [f"adv_{i:04d}" for i in range(1, 51)]    # 50 advertisers
PUBLISHERS   = [f"pub_{i:04d}" for i in range(1, 21)]    # 20 publishers
CAMPAIGNS    = [f"camp_{i:04d}" for i in range(1, 101)]  # 100 campaigns
CONSUMERS    = [str(uuid.uuid4()) for _ in range(10_000)] # 10k synthetic profiles

# ---------------------------------------------------------------------------
# GENERATOR
# ---------------------------------------------------------------------------
def generate_event(late=False):
    """Generate a single synthetic ad event."""
    event_type = random.choices(EVENT_TYPES, weights=EVENT_WEIGHTS)[0]

    # Occasionally generate late-arriving events to test watermark handling
    ts_offset = timedelta(minutes=random.randint(15, 30)) if late else timedelta(seconds=0)
    event_ts  = datetime.now(timezone.utc) - ts_offset

    return {
        "event_id":        str(uuid.uuid4()),
        "event_type":      event_type,
        "event_timestamp": event_ts.isoformat(),
        "consumer_id":     random.choice(CONSUMERS) if random.random() > 0.05 else None,
        "campaign_id":     random.choice(CAMPAIGNS),
        "advertiser_id":   random.choice(ADVERTISERS),
        "publisher_id":    random.choice(PUBLISHERS),
        "placement_id":    f"place_{random.randint(1, 500):04d}",
        "bid_price_usd":   round(random.uniform(0.01, 15.0), 4) if event_type == "impression" else None,
        "revenue_usd":     round(random.uniform(5.0, 200.0), 2) if event_type == "conversion" else None,
        "user_agent":      "Mozilla/5.0 (synthetic)",
        "ip_hash":         f"hash_{random.randint(100000, 999999)}",
        "consent_flag":    random.random() > 0.02,  # ~2% non-consented, filtered in Silver
        "geo_region":      random.choice(GEO_REGIONS),
        "device_type":     random.choices(DEVICE_TYPES, weights=[0.6, 0.35, 0.05])[0],
    }

# ---------------------------------------------------------------------------
# WRITE BATCHES
# ---------------------------------------------------------------------------
for batch_num in range(NUM_BATCHES):
    # Inject ~5% duplicate event_ids to test dedup logic in Silver
    events = [generate_event(late=(random.random() < 0.08)) for _ in range(NUM_EVENTS)]

    dupes = random.sample(events, k=int(NUM_EVENTS * 0.05))
    events.extend(dupes)
    random.shuffle(events)

    # Write as newline-delimited JSON (Auto Loader default)
    df = spark.createDataFrame([Row(**e) for e in events])
    (df.coalesce(1)
       .write
       .mode("append")
       .json(f"{OUTPUT_PATH}batch_{batch_num:04d}/"))

    print(f"[Batch {batch_num+1}/{NUM_BATCHES}] Wrote {len(events)} events "
          f"({int(NUM_EVENTS * 0.05)} dupes injected) -> {OUTPUT_PATH}")

    if batch_num < NUM_BATCHES - 1:
        time.sleep(BATCH_DELAY_S)

print("Data generation complete. Check your pipeline dashboard.")

# ---------------------------------------------------------------------------
# VERIFY RAW DATA
# Run this after generation to inspect raw event counts by type.
# ---------------------------------------------------------------------------
display(
    spark.read.json(OUTPUT_PATH)
         .groupBy("event_type", "consent_flag")
         .count()
         .orderBy("event_type")
)
