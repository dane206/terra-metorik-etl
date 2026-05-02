#!/usr/bin/env python3
"""
Terra Health Essentials — Metorik Customers API → BigQuery
==========================================================
Pulls customers from the Metorik API incrementally, using MAX(customer_updated_at)
from BQ as the resume point. Uses 7-day lookback overlap since LTV fields
recompute whenever a new order comes in.

Uses MERGE/upsert pattern on customer_id — no duplicates, no full reloads.

Table produced:
  terra-analytics-prod.sources.metorik_customers

Run modes:
  python metorik_customers_api.py --mode incremental  # default
  python metorik_customers_api.py --mode backfill     # full reload

Requires:
  config.ini with [metorik] api_key  OR  METORIK_API_KEY env var
"""

import os, sys, json, time, argparse, traceback, urllib.request, urllib.parse
from datetime import datetime, timedelta, date

import configparser
from google.cloud import bigquery

config = configparser.ConfigParser()
config.read(os.path.join(os.path.dirname(__file__), "config.ini"))

METORIK_API_KEY = os.environ.get("METORIK_API_KEY") or config.get("metorik", "api_key", fallback=None)
BQ_PROJECT      = os.environ.get("BQ_PROJECT") or config.get("bigquery", "project", fallback="terra-analytics-dev")
BQ_DATASET      = config.get("bigquery", "dataset", fallback="sources")
BQ_TABLE        = "metorik_customers"
BASE_URL        = "https://app.metorik.com/api/v1/store"
PER_PAGE        = 100
RATE_LIMIT_WAIT = 1.1

bq = bigquery.Client(project=BQ_PROJECT)

COLUMNS = [
    "customer_id", "customer_created_at", "first_order_date",
    "email", "first_name", "last_name", "role",
    "billing_address_first_name", "billing_address_last_name", "billing_address_company",
    "billing_address_address_1", "billing_address_address_2", "billing_address_city",
    "billing_address_state", "billing_address_postcode", "billing_address_country",
    "billing_address_email", "billing_address_phone",
    "geonames_id", "latitude", "longitude", "link_id",
    "customer_updated_at", "total_spent", "order_count", "item_count",
    "last_order_date", "first_paid_order_date", "days_between_orders",
    "total_shipping", "total_tax", "total_discounted", "total_refunds",
    "total_gift_cards", "tags", "total_cost", "total_profit",
]

SCHEMA = [bigquery.SchemaField(col, "STRING") for col in COLUMNS]

def s(v): return str(v) if v is not None else None

def api_get(path, params=None):
    url = f"{BASE_URL}{path}"
    if params:
        url += "?" + urllib.parse.urlencode(params)
    req = urllib.request.Request(url, headers={
        "Authorization": f"Bearer {METORIK_API_KEY}",
        "Accept": "application/json",
    })
    for attempt in range(5):
        try:
            with urllib.request.urlopen(req, timeout=30) as r:
                return json.loads(r.read())
        except urllib.error.HTTPError as e:
            if e.code == 429:
                wait = int(e.headers.get("Retry-After", 10))
                print(f"\r  Rate limited — waiting {wait}s...", end="", flush=True)
                time.sleep(wait)
            else:
                raise
    raise Exception("Max retries exceeded")

def get_resume_date():
    try:
        result = list(bq.query(
            f"SELECT MAX(customer_updated_at) AS max_date FROM `{BQ_PROJECT}.{BQ_DATASET}.{BQ_TABLE}`"
        ).result())
        val = result[0].max_date if result else None
        if val:
            dt = datetime.strptime(val[:10], "%Y-%m-%d").date() - timedelta(days=7)
            print(f"  Resuming from: {dt} (7-day overlap for LTV recompute)")
            return str(dt)
        return None
    except Exception:
        return None

def flatten_customer(c):
    ba = c.get("billing_address") or {}
    return {k: v for k, v in {
        "customer_id": s(c.get("customer_id") or c.get("id")),
        "customer_created_at": s(c.get("customer_created_at")),
        "first_order_date": s(c.get("first_order_date")),
        "email": s(c.get("email")),
        "first_name": s(c.get("first_name")),
        "last_name": s(c.get("last_name")),
        "role": s(c.get("role")),
        "billing_address_first_name": s(ba.get("first_name")),
        "billing_address_last_name": s(ba.get("last_name")),
        "billing_address_company": s(ba.get("company")),
        "billing_address_address_1": s(ba.get("address_1")),
        "billing_address_address_2": s(ba.get("address_2")),
        "billing_address_city": s(ba.get("city")),
        "billing_address_state": s(ba.get("state")),
        "billing_address_postcode": s(ba.get("postcode")),
        "billing_address_country": s(ba.get("country")),
        "billing_address_email": s(ba.get("email")),
        "billing_address_phone": s(ba.get("phone")),
        "geonames_id": s(c.get("geonames_id")),
        "latitude": s(c.get("latitude")),
        "longitude": s(c.get("longitude")),
        "link_id": s(c.get("link_id")),
        "customer_updated_at": s(c.get("customer_updated_at")),
        "total_spent": s(c.get("total_spent")),
        "order_count": s(c.get("order_count")),
        "item_count": s(c.get("item_count")),
        "last_order_date": s(c.get("last_order_date")),
        "first_paid_order_date": s(c.get("first_paid_order_date")),
        "days_between_orders": s(c.get("days_between_orders")),
        "total_shipping": s(c.get("total_shipping")),
        "total_tax": s(c.get("total_tax")),
        "total_discounted": s(c.get("total_discounted")),
        "total_refunds": s(c.get("total_refunds")),
        "total_gift_cards": s(c.get("total_gift_cards")),
        "tags": s(c.get("tags")),
        "total_cost": s(c.get("total_cost")),
        "total_profit": s(c.get("total_profit")),
    }.items() if v is not None}

def fetch_customers(updated_after=None):
    rows, page = [], 1
    params = {"per_page": PER_PAGE}
    if updated_after:
        params["updated_after"] = updated_after
    while True:
        params["page"] = page
        print(f"\r  Fetching page {page} ({len(rows):,} customers)...", end="", flush=True)
        data = api_get("/customers", params)
        batch = data.get("data") or []
        if not batch:
            break
        for c in batch:
            row = flatten_customer(c)
            if not row.get("customer_id"):
                print(f"  ⚠️  Skipping customer with no customer_id: {c.get('email')}")
                continue
            rows.append(row)
        pagination = data.get("pagination") or data.get("meta") or {}
        has_more = pagination.get("has_more_pages") or pagination.get("last_page", 1) > page
        if not has_more:
            break
        page += 1
        time.sleep(RATE_LIMIT_WAIT)
    print(f"\n  {len(rows):,} customers fetched")
    return rows

def merge_to_bq(rows):
    if not rows:
        print("  ⚠️  No rows to merge")
        return
    staging = f"{BQ_PROJECT}.{BQ_DATASET}._metorik_customers_staging"
    main    = f"{BQ_PROJECT}.{BQ_DATASET}.{BQ_TABLE}"
    print(f"  Loading {len(rows):,} rows to staging...")
    bq.load_table_from_json(rows, staging, job_config=bigquery.LoadJobConfig(
        schema=SCHEMA, write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
        ignore_unknown_values=True)).result()
    set_clause    = ",\n    ".join(f"T.`{c}` = S.`{c}`" for c in COLUMNS if c != "customer_id")
    insert_cols   = ", ".join(f"`{c}`" for c in COLUMNS)
    insert_values = ", ".join(f"S.`{c}`" for c in COLUMNS)
    bq.query(f"""
    MERGE `{main}` T USING `{staging}` S ON T.customer_id = S.customer_id
    WHEN MATCHED THEN UPDATE SET {set_clause}
    WHEN NOT MATCHED THEN INSERT ({insert_cols}) VALUES ({insert_values})
    """).result()
    bq.delete_table(staging, not_found_ok=True)
    n = list(bq.query(f"SELECT COUNT(*) AS n FROM `{main}`").result())[0].n
    print(f"  ✅ {main} — {n:,} rows total")

def main():
    if not METORIK_API_KEY:
        print("❌ METORIK_API_KEY not set")
        sys.exit(1)
    parser = argparse.ArgumentParser()
    parser.add_argument("--mode", choices=["incremental", "backfill"], default="incremental")
    args = parser.parse_args()
    print(f"🚀 Metorik Customers API → {BQ_PROJECT}.{BQ_DATASET}.{BQ_TABLE} [{args.mode}]")
    updated_after = get_resume_date() if args.mode == "incremental" else None
    if args.mode == "backfill":
        print("  Backfill: all customers")
    rows = fetch_customers(updated_after)
    print("\n💾 Merging to BigQuery...")
    merge_to_bq(rows)
    print("\n✅ Done")

if __name__ == "__main__":
    try:
        main()
    except Exception:
        print("❌ Fatal error:")
        traceback.print_exc()
        sys.exit(1)
