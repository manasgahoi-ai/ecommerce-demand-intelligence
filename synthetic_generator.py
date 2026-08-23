"""
Synthetic Data Generator — Electronics E-Commerce Demand Intelligence Platform
Calibrated against IDC India 2024 market reports & Counterpoint Research data.

Generates realistic Indian e-commerce event streams for:
  - orders-topic   : customer purchase events
  - pricing-topic  : competitor price change events

Run modes:
  python synthetic_generator.py --mode preview                # print samples, no Kafka needed
  python synthetic_generator.py --mode orders                 # stream order events only
  python synthetic_generator.py --mode pricing                # stream pricing events only
  python synthetic_generator.py --mode both                   # stream both (default)
  python synthetic_generator.py --mode backfill --days 180    # blast 180 days of history fast
  python synthetic_generator.py --mode verify                 # check what is in BQ right now
"""

import json
import os
import random
import time
import argparse
from datetime import datetime, timedelta
from dotenv import load_dotenv
from faker import Faker

# Load credentials from .env file (KAFKA_BOOTSTRAP_SERVER, KAFKA_API_KEY, KAFKA_API_SECRET).
# Confluent Cloud requires SASL_SSL — see .env.example for the template.
load_dotenv()

fake = Faker("en_IN")

# ─────────────────────────────────────────────────────────────────────────────
# PRODUCT CATALOG
#
# Weights calibrated from:
#   - IDC India Q1 2024: smartphones 28% of electronics units shipped
#   - Counterpoint Research 2024: boAt #1 audio brand by volume in India
#   - IDC Wearables 2024: smartwatches grew 18% YoY
#   - Laptop market: 12% of electronics spend (IDC India 2024)
# ─────────────────────────────────────────────────────────────────────────────
PRODUCT_CATALOG = [

    # ── Smartphones (28% of market) ───────────────────────────────────────────
    # Budget segment dominates Indian market — Redmi/Realme outsell Apple 10:1
    {"product_id": "SM001", "name": "Redmi Note 13 Pro 5G",      "category": "smartphones",  "brand": "Xiaomi",   "base_price": 18999, "weight": 11},
    {"product_id": "SM002", "name": "Samsung Galaxy A55",         "category": "smartphones",  "brand": "Samsung",  "base_price": 24999, "weight": 9},
    {"product_id": "SM003", "name": "OnePlus Nord CE4",           "category": "smartphones",  "brand": "OnePlus",  "base_price": 21999, "weight": 7},
    {"product_id": "SM004", "name": "Realme 12 Pro+ 5G",         "category": "smartphones",  "brand": "Realme",   "base_price": 22999, "weight": 8},
    {"product_id": "SM005", "name": "iQOO Z9s Pro",              "category": "smartphones",  "brand": "iQOO",     "base_price": 20999, "weight": 6},
    {"product_id": "SM006", "name": "Apple iPhone 15",           "category": "smartphones",  "brand": "Apple",    "base_price": 69999, "weight": 3},

    # ── Earbuds (22% of market — boAt dominates by volume) ───────────────────
    {"product_id": "EB001", "name": "boAt Airdopes 141",         "category": "earbuds",      "brand": "boAt",     "base_price": 999,   "weight": 16},
    {"product_id": "EB002", "name": "Noise Buds VS104 Max",      "category": "earbuds",      "brand": "Noise",    "base_price": 1299,  "weight": 12},
    {"product_id": "EB003", "name": "boAt Airdopes 800",         "category": "earbuds",      "brand": "boAt",     "base_price": 2499,  "weight": 9},
    {"product_id": "EB004", "name": "Samsung Galaxy Buds FE",    "category": "earbuds",      "brand": "Samsung",  "base_price": 4999,  "weight": 6},
    {"product_id": "EB005", "name": "Nothing Ear (2)",           "category": "earbuds",      "brand": "Nothing",  "base_price": 9999,  "weight": 4},

    # ── Headphones (10% of market) ────────────────────────────────────────────
    {"product_id": "HP001", "name": "boAt Rockerz 450 Pro",      "category": "headphones",   "brand": "boAt",     "base_price": 1299,  "weight": 11},
    {"product_id": "HP002", "name": "JBL Tune 720BT",            "category": "headphones",   "brand": "JBL",      "base_price": 3499,  "weight": 7},
    {"product_id": "HP003", "name": "Sony WH-1000XM5",           "category": "headphones",   "brand": "Sony",     "base_price": 24990, "weight": 2},
    {"product_id": "HP004", "name": "Noise One ANC",             "category": "headphones",   "brand": "Noise",    "base_price": 1799,  "weight": 9},

    # ── Smartwatches (11% of market — fastest growing segment) ───────────────
    {"product_id": "WR001", "name": "boAt Wave Sigma 2",         "category": "smartwatches", "brand": "boAt",     "base_price": 1799,  "weight": 11},
    {"product_id": "WR002", "name": "Noise ColorFit Pro 5",      "category": "smartwatches", "brand": "Noise",    "base_price": 2999,  "weight": 10},
    {"product_id": "WR003", "name": "Amazfit GTR 4",             "category": "smartwatches", "brand": "Amazfit",  "base_price": 11999, "weight": 5},
    {"product_id": "WR004", "name": "Samsung Galaxy Watch 6",    "category": "smartwatches", "brand": "Samsung",  "base_price": 22999, "weight": 3},

    # ── Laptops (12% of electronics spend) ───────────────────────────────────
    {"product_id": "LT001", "name": "Lenovo IdeaPad Slim 3 15",  "category": "laptops",      "brand": "Lenovo",   "base_price": 34999, "weight": 6},
    {"product_id": "LT002", "name": "ASUS VivoBook 16X",         "category": "laptops",      "brand": "ASUS",     "base_price": 45999, "weight": 5},
    {"product_id": "LT003", "name": "HP Victus 15 Gaming",       "category": "laptops",      "brand": "HP",       "base_price": 61999, "weight": 4},
    {"product_id": "LT004", "name": "Dell Inspiron 15 3520",     "category": "laptops",      "brand": "Dell",     "base_price": 42999, "weight": 4},

    # ── Tablets (5% of market) ────────────────────────────────────────────────
    {"product_id": "TB001", "name": "Redmi Pad SE 8.7",          "category": "tablets",      "brand": "Xiaomi",   "base_price": 12999, "weight": 7},
    {"product_id": "TB002", "name": "Samsung Galaxy Tab A9",     "category": "tablets",      "brand": "Samsung",  "base_price": 19999, "weight": 5},
    {"product_id": "TB003", "name": "Realme Pad 2",              "category": "tablets",      "brand": "Realme",   "base_price": 17499, "weight": 4},

    # ── Gaming Peripherals (3% of market — weekend spike pattern) ─────────────
    {"product_id": "GM001", "name": "Logitech G102 Gaming Mouse","category": "gaming",        "brand": "Logitech", "base_price": 1495,  "weight": 6},
    {"product_id": "GM002", "name": "Cosmic Byte CB-GK-16 Keyboard","category": "gaming",    "brand": "Cosmic Byte","base_price": 1499, "weight": 5},
    {"product_id": "GM003", "name": "HyperX Cloud Stinger 2",   "category": "gaming",        "brand": "HyperX",   "base_price": 4999,  "weight": 3},
    {"product_id": "GM004", "name": "Logitech G435 Wireless",   "category": "gaming",        "brand": "Logitech", "base_price": 5495,  "weight": 3},

    # ── Speakers (7% of market) ───────────────────────────────────────────────
    {"product_id": "SP001", "name": "boAt Stone 352 BT Speaker", "category": "speakers",     "brand": "boAt",     "base_price": 2299,  "weight": 10},
    {"product_id": "SP002", "name": "JBL Go 3",                  "category": "speakers",     "brand": "JBL",      "base_price": 3299,  "weight": 7},
    {"product_id": "SP003", "name": "Mivi Roam 2",               "category": "speakers",     "brand": "Mivi",     "base_price": 999,   "weight": 8},

    # ── Chargers & Accessories (2%) ───────────────────────────────────────────
    {"product_id": "AC001", "name": "Anker 65W GaN Charger",     "category": "chargers",     "brand": "Anker",    "base_price": 2499,  "weight": 9},
    {"product_id": "AC002", "name": "Portronics Toad 23 Mouse",  "category": "accessories",  "brand": "Portronics","base_price": 599,  "weight": 10},
    {"product_id": "AC003", "name": "Logitech MX Keys Mini",     "category": "keyboards",    "brand": "Logitech", "base_price": 7495,  "weight": 5},
]


# ─────────────────────────────────────────────────────────────────────────────
# BRAND RETURN RATES
# Source: Industry knowledge — budget brands have higher return rates due to
# impulse purchases and quality expectations mismatch
# ─────────────────────────────────────────────────────────────────────────────
BRAND_RETURN_RATES = {
    # Premium — researched purchase, low return
    "Apple":       0.03,
    "Sony":        0.03,
    "Samsung":     0.05,
    "JBL":         0.05,
    "Logitech":    0.04,
    "HyperX":      0.04,
    # Mid-tier
    "OnePlus":     0.06,
    "Nothing":     0.06,
    "Lenovo":      0.07,
    "ASUS":        0.07,
    "HP":          0.07,
    "Dell":        0.06,
    "Amazfit":     0.08,
    # Budget — impulse purchase, higher return
    "boAt":        0.10,
    "Noise":       0.11,
    "Realme":      0.09,
    "Xiaomi":      0.08,
    "iQOO":        0.08,
    "Mivi":        0.12,
    "Portronics":  0.13,
    "Cosmic Byte": 0.11,
    "Anker":       0.05,
}

# ─────────────────────────────────────────────────────────────────────────────
# CATEGORY DISCOUNT RANGES
# Budget audio: 40-60% discounts are common on Amazon India
# Premium: 5-15% typical, rarely more
# Source: Amazon Great Indian Festival & Flipkart Big Billion Days data (public)
# ─────────────────────────────────────────────────────────────────────────────
CATEGORY_DISCOUNT_PROFILE = {
    #                   (no_disc%, small%, medium%, large%, weights)
    "earbuds":      {"ranges": [0, (2,10), (10,25), (25,60)], "weights": [10, 20, 35, 35]},
    "headphones":   {"ranges": [0, (2,10), (10,25), (25,50)], "weights": [15, 25, 35, 25]},
    "smartwatches": {"ranges": [0, (2,10), (10,25), (25,55)], "weights": [10, 20, 35, 35]},
    "speakers":     {"ranges": [0, (2,10), (10,25), (25,50)], "weights": [15, 25, 35, 25]},
    "smartphones":  {"ranges": [0, (2,8),  (8,20),  (20,30)], "weights": [20, 35, 30, 15]},
    "laptops":      {"ranges": [0, (2,8),  (8,20),  (20,30)], "weights": [25, 35, 30, 10]},
    "tablets":      {"ranges": [0, (2,8),  (8,20),  (20,30)], "weights": [20, 35, 30, 15]},
    "gaming":       {"ranges": [0, (2,10), (10,20), (20,35)], "weights": [20, 30, 30, 20]},
    "chargers":     {"ranges": [0, (2,10), (10,25), (25,45)], "weights": [15, 25, 35, 25]},
    "accessories":  {"ranges": [0, (2,10), (10,25), (25,45)], "weights": [15, 25, 35, 25]},
    "keyboards":    {"ranges": [0, (2,10), (10,20), (20,35)], "weights": [20, 30, 30, 20]},
}
DEFAULT_DISCOUNT_PROFILE = {"ranges": [0, (2,10), (10,20), (20,35)], "weights": [20, 30, 30, 20]}


# ─────────────────────────────────────────────────────────────────────────────
# COMPETITOR PROFILES — each has a distinct pricing behaviour
# This makes the pricing-topic data meaningful for LightGBM features
# ─────────────────────────────────────────────────────────────────────────────
COMPETITOR_PROFILES = {
    "Amazon": {
        # Aggressively undercuts on budget products; matches price on premium
        "budget_aggression":   0.82,   # goes 18% below MRP on budget products
        "premium_aggression":  0.95,   # only 5% below on premium
        "price_change_freq":   0.40,   # 40% chance of price event per cycle
        "stock_rate":          0.92,
    },
    "Flipkart": {
        # Flash sales on weekends; mid-range discounts overall
        "budget_aggression":   0.85,
        "premium_aggression":  0.93,
        "price_change_freq":   0.35,
        "stock_rate":          0.88,
    },
    "Croma": {
        # Rarely discounts — premium retail positioning
        "budget_aggression":   0.95,
        "premium_aggression":  0.98,
        "price_change_freq":   0.15,   # price changes rarely
        "stock_rate":          0.80,
    },
    "Reliance Digital": {
        # Mid-range discounts; strong in smartphones
        "budget_aggression":   0.90,
        "premium_aggression":  0.96,
        "price_change_freq":   0.20,
        "stock_rate":          0.83,
    },
    "Vijay Sales": {
        # Regional (West India); occasional deals, often out of stock
        "budget_aggression":   0.88,
        "premium_aggression":  0.97,
        "price_change_freq":   0.18,
        "stock_rate":          0.72,   # frequently out of stock
    },
}

# ─────────────────────────────────────────────────────────────────────────────
# REGION & CITY MAPPING
# Source: Flipkart & Amazon India public seller reports — South leads electronics
# ─────────────────────────────────────────────────────────────────────────────
REGION_CITY_MAP = {
    "South": {"cities": ["Bengaluru", "Chennai", "Hyderabad", "Kochi", "Coimbatore"], "weight": 35},
    "West":  {"cities": ["Mumbai", "Pune", "Ahmedabad", "Surat", "Nagpur"],           "weight": 28},
    "North": {"cities": ["Delhi", "Gurugram", "Noida", "Jaipur", "Lucknow"],         "weight": 25},
    "East":  {"cities": ["Kolkata", "Bhubaneswar", "Patna", "Guwahati", "Ranchi"],   "weight": 12},
}

# ─────────────────────────────────────────────────────────────────────────────
# PAYMENT METHOD — varies by product price tier
# UPI dominates for sub-₹5000; EMI/Credit Card dominates for high-value
# Source: RBI Payment Data 2024
# ─────────────────────────────────────────────────────────────────────────────
PAYMENT_BY_TIER = {
    "budget":   {"UPI": 55, "Debit Card": 18, "COD": 17, "Credit Card": 7,  "EMI": 3},
    "mid":      {"UPI": 45, "Debit Card": 14, "COD": 12, "Credit Card": 18, "EMI": 11},
    "premium":  {"UPI": 20, "Debit Card": 8,  "COD": 5,  "Credit Card": 32, "EMI": 35},
}


# ─────────────────────────────────────────────────────────────────────────────
# HELPER UTILITIES
# ─────────────────────────────────────────────────────────────────────────────

def weighted_choice(options_dict):
    keys = list(options_dict.keys())
    weights = list(options_dict.values())
    return random.choices(keys, weights=weights, k=1)[0]


def pick_product():
    weights = [p["weight"] for p in PRODUCT_CATALOG]
    return random.choices(PRODUCT_CATALOG, weights=weights, k=1)[0]


def pick_region_and_city():
    regions = list(REGION_CITY_MAP.keys())
    region_weights = [REGION_CITY_MAP[r]["weight"] for r in regions]
    region = random.choices(regions, weights=region_weights, k=1)[0]
    city = random.choice(REGION_CITY_MAP[region]["cities"])
    return region, city


def price_tier(base_price):
    """Classify product into budget / mid / premium for payment method selection."""
    if base_price < 5000:
        return "budget"
    elif base_price < 25000:
        return "mid"
    return "premium"


def realistic_price(base_price):
    """Dynamic pricing variance — simulates real-time price fluctuation."""
    return round(base_price * random.uniform(0.90, 1.06), 2)


def category_discount(category):
    """Category-specific discount distribution — budget audio gets 40-60% off."""
    profile = CATEGORY_DISCOUNT_PROFILE.get(category, DEFAULT_DISCOUNT_PROFILE)
    bucket = random.choices(profile["ranges"], weights=profile["weights"], k=1)[0]
    if bucket == 0:
        return 0.0
    return round(random.uniform(bucket[0], bucket[1]), 1)


def brand_return_rate(brand):
    """Brand-specific return rate — premium brands have lower returns."""
    return BRAND_RETURN_RATES.get(brand, 0.08)


def customer_segment():
    return random.choices(["new", "returning", "premium"], weights=[40, 45, 15], k=1)[0]


def is_weekend_spike(category):
    """Gaming peripherals and budget audio spike on weekends."""
    today = datetime.now().weekday()   # 5=Sat, 6=Sun
    if today in (5, 6) and category in ("gaming", "earbuds", "headphones"):
        return random.random() < 0.65  # 65% chance extra order on weekend
    return True


def simulate_timestamp(mode="live", base_dt=None):
    """
    live    → current time (streaming mode)
    backfill → uses base_dt passed in from backfill loop
    """
    if mode == "backfill" and base_dt:
        # Add a random hour offset so events within same day are spread out
        offset_hours = random.uniform(0, 23)
        return (base_dt + timedelta(hours=offset_hours)).isoformat()
    return datetime.now().isoformat()


def competitor_price(base_price, category, competitor_name):
    """
    Compute competitor price using that competitor's behavioural profile.
    Budget products get more aggressive discounting.
    """
    profile = COMPETITOR_PROFILES[competitor_name]
    tier = price_tier(base_price)
    if tier == "budget":
        factor = random.uniform(profile["budget_aggression"], 1.05)
    elif tier == "premium":
        factor = random.uniform(profile["premium_aggression"], 1.02)
    else:
        aggression = (profile["budget_aggression"] + profile["premium_aggression"]) / 2
        factor = random.uniform(aggression, 1.04)
    return round(base_price * factor, 2)


# ─────────────────────────────────────────────────────────────────────────────
# EVENT GENERATORS
# ─────────────────────────────────────────────────────────────────────────────

def generate_order_event(ts_mode="live", base_dt=None):
    """Generates one order event → orders-topic"""
    product = pick_product()
    region, city = pick_region_and_city()

    # Gaming peripherals sell more in larger quantities (tournament setups)
    if product["category"] == "gaming":
        quantity = random.choices([1, 2, 3, 4], weights=[50, 30, 12, 8], k=1)[0]
    else:
        quantity = random.choices([1, 2, 3, 4, 5], weights=[60, 25, 8, 4, 3], k=1)[0]

    final_price = realistic_price(product["base_price"])
    discount    = category_discount(product["category"])
    tier        = price_tier(product["base_price"])
    return_prob = brand_return_rate(product["brand"])

    return {
        "event_type":       "order_placed",
        "order_id":         f"ORD-{fake.bothify('??####??####').upper()}",
        "product_id":       product["product_id"],
        "product_name":     product["name"],
        "category":         product["category"],
        "brand":            product["brand"],
        "price_tier":       tier,
        "base_price":       product["base_price"],
        "final_price":      final_price,
        "discount_percent": discount,
        "quantity":         quantity,
        "total_value":      round(final_price * quantity, 2),
        "region":           region,
        "city":             city,
        "payment_method":   weighted_choice(PAYMENT_BY_TIER[tier]),
        "customer_segment": customer_segment(),
        "is_returned":      random.random() < return_prob,
        "timestamp":        simulate_timestamp(ts_mode, base_dt),
    }


def generate_pricing_event(ts_mode="live", base_dt=None):
    """Generates one competitor price update → pricing-topic"""
    product  = pick_product()
    comp_name = random.choice(list(COMPETITOR_PROFILES.keys()))
    profile  = COMPETITOR_PROFILES[comp_name]

    # Respect competitor's price change frequency
    if random.random() > profile["price_change_freq"]:
        return None   # this competitor didn't change price this cycle

    comp_price = competitor_price(product["base_price"], product["category"], comp_name)
    our_price  = realistic_price(product["base_price"])
    price_diff = round(our_price - comp_price, 2)

    return {
        "event_type":       "price_update",
        "product_id":       product["product_id"],
        "product_name":     product["name"],
        "category":         product["category"],
        "brand":            product["brand"],
        "competitor":       comp_name,
        "competitor_price": comp_price,
        "our_price":        our_price,
        "price_diff":       price_diff,
        "price_diff_pct":   round((price_diff / our_price) * 100, 2),
        "in_stock":         random.random() < profile["stock_rate"],
        "timestamp":        simulate_timestamp(ts_mode, base_dt),
    }


# ─────────────────────────────────────────────────────────────────────────────
# KAFKA PRODUCER
# ─────────────────────────────────────────────────────────────────────────────

# Default local Docker bootstrap. The Docker broker advertises `kafka:9092`,
# which resolves on the Windows host via the entry added to the hosts file:
#   127.0.0.1 kafka
# (see docker-compose.yml header for the one-time setup command).
DEFAULT_BOOTSTRAP = "kafka:9092"


def make_producer(bootstrap_servers=None):
    """
    Build a KafkaProducer.

    Two modes are supported (auto-detected):

    1. Local Docker Kafka (default)
       bootstrap_servers="kafka:9092" or use the default.
       Requires the hosts-file entry: 127.0.0.1 kafka

    2. Confluent Cloud (SASL_SSL)
       Set KAFKA_BOOTSTRAP_SERVER, KAFKA_API_KEY, KAFKA_API_SECRET in .env
       and pass bootstrap_servers=None (it will pick them up from env).
    """
    try:
        from kafka import KafkaProducer
    except ImportError:
        print("kafka-python not installed. Run: pip install kafka-python")
        raise

    # If credentials are in env, use Confluent Cloud mode
    env_bootstrap = os.getenv("KAFKA_BOOTSTRAP_SERVER")
    api_key = os.getenv("KAFKA_API_KEY")
    api_secret = os.getenv("KAFKA_API_SECRET")

    if env_bootstrap and api_key and api_secret:
        # Confluent Cloud — SASL_SSL auth
        return KafkaProducer(
            bootstrap_servers=env_bootstrap,
            security_protocol="SASL_SSL",
            sasl_mechanism="PLAIN",
            sasl_plain_username=api_key,
            sasl_plain_password=api_secret,
            value_serializer=lambda v: json.dumps(v).encode("utf-8"),
        )

    # Otherwise — local Docker, plaintext
    return KafkaProducer(
        bootstrap_servers=bootstrap_servers or DEFAULT_BOOTSTRAP,
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
    )


def start_kafka_stream(mode="both", delay=0.5):
    """Streams live events into Kafka (local Docker or Confluent Cloud)."""
    try:
        from kafka import KafkaProducer   # imported only to surface a clean error
    except ImportError:
        print("kafka-python not installed. Run: pip install kafka-python")
        return

    producer = make_producer()

    print(f"\n🚀 Streaming started | mode={mode} | delay={delay}s")
    print("Press Ctrl+C to stop\n")

    count = 0
    try:
        while True:
            if mode in ("orders", "both"):
                event = generate_order_event()
                producer.send("orders-topic", value=event)
                count += 1
                print(f"[orders  #{count:>5}] {event['product_name'][:35]:<35} "
                      f"₹{event['final_price']:>8} | {event['region']:<6} | "
                      f"{event['payment_method']}")

            if mode in ("pricing", "both"):
                event = generate_pricing_event()
                if event:
                    producer.send("pricing-topic", value=event)
                    indicator = "✅ cheaper" if event["price_diff"] > 0 else "⚠️ pricier"
                    print(f"[pricing        ] {event['competitor']:<18} "
                          f"₹{event['competitor_price']:>8} vs us ₹{event['our_price']:>8} "
                          f"→ {indicator}")

            producer.flush()
            time.sleep(delay)

    except KeyboardInterrupt:
        print(f"\n⏹ Stream stopped. Total order events sent: {count}")
        producer.close()


# ─────────────────────────────────────────────────────────────────────────────
# BACKFILL MODE — blast historical data into Kafka at full speed
# Run this once before Week 3 ML training to populate BigQuery with history
# ─────────────────────────────────────────────────────────────────────────────

def run_backfill(days=180, events_per_day=500):
    """
    Generates `days` × `events_per_day` historical events at full speed.
    Timestamps are spread across past N days — gives LightGBM real temporal patterns.
    """
    producer = make_producer()

    total = days * events_per_day
    print(f"\n⏩ Backfill started: {days} days × {events_per_day} events/day = {total:,} total")
    print("No delay — running at full speed. This will take ~30–60 seconds.\n")

    count = 0
    start = time.time()

    for day_offset in range(days, 0, -1):     # oldest first → newest last
        base_dt = datetime.now() - timedelta(days=day_offset)

        for _ in range(events_per_day):
            event = generate_order_event(ts_mode="backfill", base_dt=base_dt)
            producer.send("orders-topic", value=event)
            count += 1

            # Occasional pricing event
            if random.random() < 0.2:
                p_event = generate_pricing_event(ts_mode="backfill", base_dt=base_dt)
                if p_event:
                    producer.send("pricing-topic", value=p_event)

        if day_offset % 30 == 0:
            elapsed = time.time() - start
            print(f"  Progress: {days - day_offset}/{days} days | "
                  f"{count:,} events | {elapsed:.1f}s elapsed")

    producer.flush()
    producer.close()
    elapsed = time.time() - start
    print(f"\n✅ Backfill complete: {count:,} events in {elapsed:.1f}s")


# ─────────────────────────────────────────────────────────────────────────────
# PREVIEW MODE — no Kafka needed, validates event schemas
# ─────────────────────────────────────────────────────────────────────────────

def preview_events(n=3):
    print("\n" + "="*65)
    print("  ORDERS-TOPIC  —  Sample Events")
    print("="*65)
    for i in range(n):
        event = generate_order_event()
        print(f"\n[Event {i+1}]")
        print(json.dumps(event, indent=2))

    print("\n" + "="*65)
    print("  PRICING-TOPIC  —  Sample Events (competitor behaviours)")
    print("="*65)
    shown = 0
    attempts = 0
    while shown < n and attempts < 50:
        event = generate_pricing_event()
        attempts += 1
        if event:
            print(f"\n[Event {shown+1}] — Competitor: {event['competitor']}")
            print(json.dumps(event, indent=2))
            shown += 1

def run_direct_backfill(days=180, events_per_day=500):
    """
    Writes historical events directly to BigQuery.
    No Kafka needed — bypasses streaming entirely for historical load.

    Each load job's output_rows is reported and accumulated so that the final
    count is the *real* number of rows the BQ API confirmed it wrote, not
    just the size of the in-memory buffer. The script also queries BQ after
    the run to confirm what's actually there.
    """
    from google.cloud import bigquery
    import pandas as pd

    project = os.getenv("GCP_PROJECT", "demand-intelligence-504514")
    dataset = os.getenv("BQ_DATASET", "ecommerce_events")
    orders_table   = f"{project}.{dataset}.orders"
    pricing_table  = f"{project}.{dataset}.pricing_events"

    bq = bigquery.Client(project=project)

    # Tell the user exactly where we are writing — this catches "wrong project"
    # and "wrong table" mistakes up front.
    print(f"\n⏩ Direct BigQuery backfill: {days} days × {events_per_day} events")
    print(f"   Orders  → {orders_table}")
    print(f"   Pricing → {pricing_table}")
    print(f"   Writing in batches of 1000 with per-batch row-count verification.\n")

    # Pre-flight: report what is already in BQ so the user can see the delta
    for tbl in (orders_table, pricing_table):
        try:
            t = bq.get_table(tbl)
            print(f"   ℹ️  {tbl} currently has {t.num_rows:,} rows")
        except Exception:
            print(f"   ℹ️  {tbl} does not exist yet (will be created on first batch)")

    orders_buffer  = []
    pricing_buffer = []
    BATCH_SIZE     = 1000

    total_days     = days
    total_orders   = 0
    total_pricing  = 0
    orders_loaded  = 0    # rows the BQ API confirmed it wrote
    pricing_loaded = 0
    batch_count    = 0
    failed_batches = 0

    def flush(buffer, table_id, kind):
        nonlocal orders_loaded, pricing_loaded, failed_batches
        if not buffer:
            return
        df = pd.DataFrame(buffer)

        # Explicit timestamp conversion with UTC timezone
        df["timestamp"] = pd.to_datetime(df["timestamp"], format="ISO8601", utc=True)

        # Booleans must be explicit — pandas infers object for some bool values
        if "is_returned" in df.columns:
            df["is_returned"] = df["is_returned"].astype(bool)
        if "in_stock" in df.columns:
            df["in_stock"] = df["in_stock"].astype(bool)

        # WRITE_APPEND only — never let pandas_gbq silently rewrite the table.
        # autodetect_schema=True so the table gets created with the right types
        # on the first batch and matches subsequent batches.
        job_config = bigquery.LoadJobConfig(
            write_disposition=bigquery.WriteDisposition.WRITE_APPEND,
            autodetect=True,
        )

        try:
            job = bq.load_table_from_dataframe(df, table_id, job_config=job_config)
            result = job.result()                              # blocks until done
            written = result.output_rows or len(df)            # output_rows = BQ-confirmed count
            if kind == "orders":
                orders_loaded += written
            else:
                pricing_loaded += written
        except Exception as e:
            failed_batches += 1
            print(f"\n❌ Batch write to {table_id} failed: {type(e).__name__}: {e}")
            print(f"   Buffered rows: {len(df)}")
            print(f"   Sample row: {df.iloc[0].to_dict()}")
            raise

        buffer.clear()

    for day_offset in range(days, 0, -1):
        base_dt = datetime.now() - timedelta(days=day_offset)

        for _ in range(events_per_day):
            order = generate_order_event(ts_mode="backfill", base_dt=base_dt)
            orders_buffer.append(order)
            total_orders += 1

            # generate_pricing_event() returns None when the chosen competitor
            # didn't change price that cycle (controlled by price_change_freq).
            # Don't add an extra outer gate here — that would double-gate and
            # suppress ~75% of pricing events.
            pricing = generate_pricing_event(ts_mode="backfill", base_dt=base_dt)
            if pricing:
                pricing_buffer.append(pricing)
                total_pricing += 1

            if len(orders_buffer) >= BATCH_SIZE:
                flush(orders_buffer, orders_table, "orders")
                batch_count += 1
                print(f"  ✅ batch {batch_count:>3} | "
                      f"buffered {total_orders:>7,} | "
                      f"BQ-confirmed {orders_loaded:>7,} orders | "
                      f"day {total_days - day_offset + 1}/{total_days}")

            if len(pricing_buffer) >= BATCH_SIZE:
                flush(pricing_buffer, pricing_table, "pricing")

    # Flush remaining
    if orders_buffer:
        flush(orders_buffer, orders_table, "orders")
        batch_count += 1
        print(f"  ✅ batch {batch_count:>3} (final) | "
              f"buffered {total_orders:>7,} | "
              f"BQ-confirmed {orders_loaded:>7,} orders")
    if pricing_buffer:
        flush(pricing_buffer, pricing_table, "pricing")

    print(f"\n📊 Backfill finished.")
    print(f"   Rows generated in memory:  {total_orders:,} orders, {total_pricing:,} pricing")
    print(f"   Rows confirmed by BQ API:  {orders_loaded:,} orders, {pricing_loaded:,} pricing")
    print(f"   Failed batches:            {failed_batches}")
    if total_orders != orders_loaded:
        print(f"   ⚠️  MISMATCH: generated {total_orders:,} but BQ confirmed {orders_loaded:,}")
        print(f"      Diff = {total_orders - orders_loaded:,} rows")
    print()

    # POST-LOAD VERIFICATION — actually query BQ, don't trust the in-memory counter
    print("🔍 Verifying with live BQ query...")
    for label, tbl in [("orders", orders_table), ("pricing", pricing_table)]:
        try:
            row_count = bq.query(f"SELECT COUNT(*) AS n FROM `{tbl}`").result().to_dataframe().iloc[0]["n"]
            min_ts = bq.query(f"SELECT MIN(timestamp) AS mn FROM `{tbl}`").result().to_dataframe().iloc[0]["mn"]
            max_ts = bq.query(f"SELECT MAX(timestamp) AS mx FROM `{tbl}`").result().to_dataframe().iloc[0]["mx"]
            print(f"   {tbl}: {int(row_count):,} rows | {min_ts} → {max_ts}")
        except Exception as e:
            print(f"   {label}: verification query failed — {e}")


def run_verify():
    """Query BigQuery and report what's actually in the orders + pricing tables."""
    from google.cloud import bigquery

    project = os.getenv("GCP_PROJECT", "demand-intelligence-504514")
    dataset = os.getenv("BQ_DATASET", "ecommerce_events")
    bq = bigquery.Client(project=project)

    print("\n🔍 BigQuery verification\n")
    for tbl in [f"{project}.{dataset}.orders", f"{project}.{dataset}.pricing_events"]:
        print(f"--- {tbl} ---")
        try:
            t = bq.get_table(tbl)
            print(f"  Rows: {t.num_rows:,}")
        except Exception as e:
            print(f"  Table missing or inaccessible: {e}")
            continue
        # Summary stats
        for q, label in [
            (f"SELECT COUNT(*) AS n FROM `{tbl}`", "COUNT(*)"),
            (f"SELECT MIN(timestamp) AS mn, MAX(timestamp) AS mx FROM `{tbl}`", "date range"),
            (f"SELECT DATE(timestamp) AS d, COUNT(*) AS n FROM `{tbl}` "
             f"GROUP BY d ORDER BY d", "rows per day"),
        ]:
            try:
                df = bq.query(q).result().to_dataframe()
                if label == "rows per day":
                    print(f"  rows per day (first 5 + last 5):")
                    if len(df) > 10:
                        print(df.head(5).to_string(index=False))
                        print("    ...")
                        print(df.tail(5).to_string(index=False))
                    else:
                        print(df.to_string(index=False))
                else:
                    print(f"  {label}: {df.iloc[0].to_dict()}")
            except Exception as e:
                print(f"  {label}: query failed — {e}")
        print()


# ─────────────────────────────────────────────────────────────────────────────
# ENTRY POINT
# ─────────────────────────────────────────────────────────────────────────────

if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Electronics E-Commerce Synthetic Data Generator"
    )
    parser.add_argument(
        "--mode",
        choices=["orders", "pricing", "both", "preview", "backfill", "verify"],
        default="preview",
        help="Run mode (default: preview). 'verify' queries BQ to show what is actually there."
    )
    parser.add_argument(
        "--delay", type=float, default=0.5,
        help="Seconds between events in streaming mode (default: 0.5)"
    )
    parser.add_argument(
        "--preview-count", type=int, default=3,
        help="Number of sample events in preview mode (default: 3)"
    )
    parser.add_argument(
        "--days", type=int, default=180,
        help="Days of history to generate in backfill mode (default: 180)"
    )
    parser.add_argument(
        "--events-per-day", type=int, default=500,
        help="Events per day in backfill mode (default: 500)"
    )
    args = parser.parse_args()

    if args.mode == "preview":
        preview_events(n=args.preview_count)
    elif args.mode == "backfill":
        run_direct_backfill(days=args.days, events_per_day=args.events_per_day)
    elif args.mode == "verify":
        run_verify()
    else:
        start_kafka_stream(mode=args.mode, delay=args.delay)
