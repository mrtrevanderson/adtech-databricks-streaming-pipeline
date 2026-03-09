# Acme Media - E-Commerce Event Data Generator
# ================================================
# Simulates an e-commerce website streaming CSV event files into the
# Auto Loader landing zone. Generates two data sources:
#   1. ecommerce_events  -- user clickstream (page_view, add_to_cart, purchase, etc.)
#   2. user_profiles     -- consumer identity records with CDC operations
#
# Run this notebook alongside the pipeline to observe real-time ingestion.
# The generator injects realistic patterns:
#   - Full purchase funnels (browse -> cart -> checkout -> buy)
#   - Abandoned sessions (browse only, no purchase)
#   - Late-arriving events (to test watermark handling)
#   - Duplicate event_ids (to test dedup logic)
#   - CDC profile updates (INSERT then UPDATE for same user)

import uuid
import csv
import random
import time
import io
from datetime import datetime, timezone, timedelta
import time as _time
from pyspark.sql import Row

# ---------------------------------------------------------------------------
# CONFIG
# ---------------------------------------------------------------------------
EVENTS_PATH   = "/Volumes/ius_unity_prod/sandbox/ecommerce_events/"
PROFILES_PATH = "/Volumes/ius_unity_prod/sandbox/user_profiles/"

# Unique run ID based on timestamp -- ensures every run produces new file paths
# so Auto Loader always detects them as new files
RUN_ID = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
NUM_BATCHES   = 10
BATCH_DELAY_S = 5

# Reference data
PRODUCTS = [
    ("prod_101", "footwear",    89.99),
    ("prod_102", "footwear",   129.99),
    ("prod_201", "apparel",     59.99),
    ("prod_202", "apparel",    149.00),
    ("prod_301", "accessories", 45.00),
    ("prod_302", "accessories", 299.00),
    ("prod_401", "electronics", 499.99),
    ("prod_501", "beauty",      35.00),
]
DEVICE_TYPES = ["mobile", "desktop", "tablet"]
GEO_REGIONS  = ["NY", "CA", "TX", "FL", "IL", "WA", "MA", "GA"]
USERS        = [f"usr_{i:04d}" for i in range(1, 201)]  # 200 synthetic users

# ---------------------------------------------------------------------------
# SESSION GENERATOR
# Generates a realistic funnel: page_view(s) -> maybe add_to_cart -> maybe purchase
# ---------------------------------------------------------------------------
def generate_session(late=False):
    session_id = f"sess_{uuid.uuid4().hex[:8]}"
    user_id    = random.choice(USERS) if random.random() > 0.1 else None  # 10% anonymous
    device     = random.choice(DEVICE_TYPES)
    geo        = random.choice(GEO_REGIONS)
    consent    = random.random() > 0.03  # 3% non-consented
    product    = random.choice(PRODUCTS)
    base_ts    = datetime.now(timezone.utc)
    if late:
        base_ts -= timedelta(minutes=random.randint(16, 25))

    events = []
    ts = base_ts

    # Always start with a page view
    events.append({
        "event_id":         str(uuid.uuid4()),
        "event_timestamp":  ts.strftime("%Y-%m-%d %H:%M:%S"),
        "session_id":       session_id,
        "user_id":          user_id,
        "event_type":       "page_view",
        "page_url":         "/home",
        "product_id":       "",
        "product_category": "",
        "product_price":    "",
        "quantity":         "",
        "order_id":         "",
        "consent_flag":     str(consent).lower(),
        "device_type":      device,
        "geo_region":       geo,
        "ip_hash":          f"hash_{random.randint(100000,999999)}",
    })

    # Product page view
    ts += timedelta(seconds=random.randint(20, 90))
    events.append({
        "event_id":         str(uuid.uuid4()),
        "event_timestamp":  ts.strftime("%Y-%m-%d %H:%M:%S"),
        "session_id":       session_id,
        "user_id":          user_id,
        "event_type":       "page_view",
        "page_url":         f"/products/{product[1]}/{product[0]}",
        "product_id":       product[0],
        "product_category": product[1],
        "product_price":    str(product[2]),
        "quantity":         "",
        "order_id":         "",
        "consent_flag":     str(consent).lower(),
        "device_type":      device,
        "geo_region":       geo,
        "ip_hash":          f"hash_{random.randint(100000,999999)}",
    })

    # 60% chance of add to cart
    if random.random() < 0.60:
        ts += timedelta(seconds=random.randint(10, 60))
        qty = random.randint(1, 3)
        events.append({
            "event_id":         str(uuid.uuid4()),
            "event_timestamp":  ts.strftime("%Y-%m-%d %H:%M:%S"),
            "session_id":       session_id,
            "user_id":          user_id,
            "event_type":       "add_to_cart",
            "page_url":         f"/products/{product[1]}/{product[0]}",
            "product_id":       product[0],
            "product_category": product[1],
            "product_price":    str(product[2]),
            "quantity":         str(qty),
            "order_id":         "",
            "consent_flag":     str(consent).lower(),
            "device_type":      device,
            "geo_region":       geo,
            "ip_hash":          f"hash_{random.randint(100000,999999)}",
        })

        # 50% of carts go to checkout
        if random.random() < 0.50:
            ts += timedelta(seconds=random.randint(30, 120))
            events.append({
                "event_id":         str(uuid.uuid4()),
                "event_timestamp":  ts.strftime("%Y-%m-%d %H:%M:%S"),
                "session_id":       session_id,
                "user_id":          user_id,
                "event_type":       "checkout_start",
                "page_url":         "/checkout",
                "product_id":       product[0],
                "product_category": product[1],
                "product_price":    str(product[2]),
                "quantity":         str(qty),
                "order_id":         "",
                "consent_flag":     str(consent).lower(),
                "device_type":      device,
                "geo_region":       geo,
                "ip_hash":          f"hash_{random.randint(100000,999999)}",
            })

            # 70% of checkouts complete purchase
            if random.random() < 0.70:
                ts += timedelta(seconds=random.randint(30, 90))
                events.append({
                    "event_id":         str(uuid.uuid4()),
                    "event_timestamp":  ts.strftime("%Y-%m-%d %H:%M:%S"),
                    "session_id":       session_id,
                    "user_id":          user_id,
                    "event_type":       "purchase",
                    "page_url":         "/order-confirm",
                    "product_id":       product[0],
                    "product_category": product[1],
                    "product_price":    str(product[2]),
                    "quantity":         str(qty),
                    "order_id":         f"ord_{uuid.uuid4().hex[:8]}",
                    "consent_flag":     str(consent).lower(),
                    "device_type":      device,
                    "geo_region":       geo,
                    "ip_hash":          f"hash_{random.randint(100000,999999)}",
                })
    return events


# ---------------------------------------------------------------------------
# PROFILE GENERATOR
# ---------------------------------------------------------------------------
INTERESTS_POOL     = ["footwear", "fitness", "apparel", "travel", "tech", "accessories", "beauty", "luxury", "outdoor"]
LOYALTY_TIERS      = ["bronze", "silver", "gold", "platinum"]
INCOME_BANDS       = ["under_25k", "25k-50k", "50k-75k", "75k-100k", "100k+"]
AGE_BANDS          = ["18-24", "25-34", "35-44", "45-54", "55+"]

def generate_profile(user_id, operation="INSERT"):
    interests = random.sample(INTERESTS_POOL, k=random.randint(2, 5))
    tier      = random.choices(LOYALTY_TIERS, weights=[0.40, 0.30, 0.20, 0.10])[0]
    ltv       = round(random.uniform(50, 5000), 2)
    return {
        "user_id":                user_id,
        "email_hash":             f"hash_email_{user_id}",
        "age_band":               random.choice(AGE_BANDS),
        "gender":                 random.choice(["M", "F", "NB"]),
        "income_band":            random.choice(INCOME_BANDS),
        "interests":              ",".join(interests),
        "loyalty_tier":           tier,
        "lifetime_value_usd":     str(ltv),
        "preferred_categories":   ",".join(random.sample(interests, k=min(3, len(interests)))),
        "last_purchase_category": random.choice(interests),
        "total_orders":           str(random.randint(0, 30)),
        "consent_flag":           "true",
        "operation":              operation,
        "updated_at":             datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S"),
    }


# ---------------------------------------------------------------------------
# WRITE TO VOLUMES AS CSV
# ---------------------------------------------------------------------------
EVENT_COLUMNS = [
    "event_id", "event_timestamp", "session_id", "user_id", "event_type",
    "page_url", "product_id", "product_category", "product_price", "quantity",
    "order_id", "consent_flag", "device_type", "geo_region", "ip_hash"
]
PROFILE_COLUMNS = [
    "user_id", "email_hash", "age_band", "gender", "income_band", "interests",
    "loyalty_tier", "lifetime_value_usd", "preferred_categories",
    "last_purchase_category", "total_orders", "consent_flag", "operation", "updated_at"
]

def write_csv_to_volume(rows, columns, path, filename):
    output = io.StringIO()
    writer = csv.DictWriter(output, fieldnames=columns, extrasaction="ignore")
    writer.writeheader()
    writer.writerows(rows)
    # Write a single clean .csv file using dbutils — no Spark subdirectories or _SUCCESS files
    dbutils.fs.put(f"{path}{filename}.csv", output.getvalue(), overwrite=True)
    return len(rows)


for batch_num in range(NUM_BATCHES):
    print(f"\n--- Batch {batch_num + 1}/{NUM_BATCHES} ---")

    # Generate 20-40 sessions worth of events
    all_events = []
    num_sessions = random.randint(20, 40)
    for _ in range(num_sessions):
        late = random.random() < 0.08  # 8% late-arriving
        all_events.extend(generate_session(late=late))

    # Inject 5% duplicates to test dedup
    dupes = random.sample(all_events, k=max(1, len(all_events) // 20))
    all_events.extend(dupes)
    random.shuffle(all_events)

    n = write_csv_to_volume(all_events, EVENT_COLUMNS, EVENTS_PATH, f"run_{RUN_ID}_batch_{batch_num:04d}")
    print(f"  Events written: {n} ({len(dupes)} dupes injected)")

    # Every other batch, send profile updates
    if batch_num % 2 == 0:
        sample_users  = random.sample(USERS, k=20)
        profiles      = [generate_profile(u, "INSERT") for u in sample_users[:10]]
        profiles     += [generate_profile(u, "UPDATE") for u in sample_users[10:]]
        m = write_csv_to_volume(profiles, PROFILE_COLUMNS, PROFILES_PATH, f"run_{RUN_ID}_profiles_{batch_num:04d}")
        print(f"  Profiles written: {m} (10 INSERTs, 10 UPDATEs)")

    if batch_num < NUM_BATCHES - 1:
        time.sleep(BATCH_DELAY_S)

print("\nData generation complete. Start the pipeline and run the validation notebook.")
