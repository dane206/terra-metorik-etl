#!/usr/bin/env python3
"""
Terra Health Essentials — Metorik Ad Spend → BigQuery
======================================================
Pulls daily ad spend by platform/account from the Metorik API.

Table produced:
  terra-analytics-prod.sources.metorik_ad_spend_daily

Run modes:
  python metorik_ad_spend.py --mode backfill    # 2022-08-20 → yesterday
  python metorik_ad_spend.py --mode incremental # last 7 days (default)

Requires:
  config.ini with [metorik] api_key
"""

import os, sys, argparse, traceback, json, urllib.request, urllib.parse
from datetime import date, timedelta, datetime, timezone

import configparser
from google.cloud import bigquery

config = configparser.ConfigParser()
config.read(os.path.join(os.path.dirname(__file__), "config.ini"))

# ── Config ────────────────────────────────────────────────────────────────────
METORIK_API_KEY = os.environ.get("METORIK_API_KEY") or config.get("metorik", "api_key", fallback=None)
EARLIEST_DATE   = "2022-08-20"
BQ_PROJECT      = os.environ.get("BQ_PROJECT", "terra-analytics-prod")
BQ_DATASET      = "sources"
BQ_TABLE        = "metorik_ad_spend_daily"

bq = bigquery.Client(project=BQ_PROJECT)

# ── Schema ────────────────────────────────────────────────────────────────────
SF = bigquery.SchemaField
schema = [
    SF("date",        "DATE"),
    SF("method_id",   "STRING"),
    SF("method_name", "STRING"),
    SF("spend",       "FLOAT64"),
    SF("_loaded_at",  "TIMESTAMP"),
]

def get_max_date(table):
    """Return MAX(date) from a BQ table as a string, or None if empty."""
    try:
        result = list(bq.query(f"SELECT CAST(MAX(date) AS STRING) AS max_date FROM `{BQ_PROJECT}.{BQ_DATASET}.{table}`").result())
        val = result[0].max_date if result else None
        if val:
            print(f"  Resuming from: {val}")
        return val
    except Exception:
        return None

# ── API ───────────────────────────────────────────────────────────────────────
def fetch_ad_spend(start_date: str, end_date: str) -> dict:
    params = urllib.parse.urlencode({
        "start_date": start_date,
        "end_date":   end_date,
        "group_by":   "day",
    })
    url = f"https://app.metorik.com/api/v1/store/reports/advertising-costs-by-date?{params}"
    req = urllib.request.Request(url, headers={
        "Authorization": f"Bearer {METORIK_API_KEY}",
        "Accept":        "application/json",
    })
    with urllib.request.urlopen(req) as r:
        return json.loads(r.read())

# ── Transform ─────────────────────────────────────────────────────────────────
def parse_rows(data: dict) -> list:
    methods   = data["meta"]["methods"]
    loaded_at = datetime.now(timezone.utc).isoformat()
    rows      = []

    for day in data["data"]:
        for method_id, spend in day["methods"].items():
            rows.append({
                "date":        day["date"],
                "method_id":   method_id,
                "method_name": methods.get(method_id, method_id),
                "spend":       float(spend),
                "_loaded_at":  loaded_at,
            })

    return rows

# ── Load ──────────────────────────────────────────────────────────────────────
def load_to_bq(rows: list, write_mode):
    if not rows:
        print("  ⚠️  No rows to load")
        return
    table_id = f"{BQ_PROJECT}.{BQ_DATASET}.{BQ_TABLE}"
    job = bq.load_table_from_json(rows, table_id, job_config=bigquery.LoadJobConfig(
        schema=schema,
        write_disposition=write_mode,
        ignore_unknown_values=True,
    ))
    job.result()
    print(f"  ✅ {table_id} — {bq.get_table(table_id).num_rows:,} rows")

# ── Main ──────────────────────────────────────────────────────────────────────
def main():
    if not METORIK_API_KEY:
        print("❌ METORIK_API_KEY not set in .env")
        sys.exit(1)

    parser = argparse.ArgumentParser()
    parser.add_argument("--mode", choices=["backfill", "incremental"], default="incremental")
    args = parser.parse_args()

    yesterday = str(date.today() - timedelta(days=1))

    if args.mode == "incremental":
        max_date = get_max_date(BQ_TABLE)
        start_date = str((datetime.strptime(max_date, "%Y-%m-%d").date() - timedelta(days=1))) if max_date else str(date.today() - timedelta(days=7))
        write_mode = bigquery.WriteDisposition.WRITE_APPEND
    else:
        start_date = EARLIEST_DATE
        write_mode = bigquery.WriteDisposition.WRITE_TRUNCATE

    mode_label = "backfill" if args.mode == "backfill" else "incremental"
    print(f"🚀 Metorik Ad Spend {mode_label}: {start_date} → {yesterday}")

    print(f"  Fetching {start_date} → {yesterday}...")
    data = fetch_ad_spend(start_date, yesterday)

    if data["meta"].get("results_limited"):
        print("  ⚠️  results_limited=true — data may be truncated")

    rows = parse_rows(data)
    print(f"  {len(rows):,} rows parsed ({len(data['data'])} days × methods)")

    print("\n💾 Loading to BigQuery...")
    load_to_bq(rows, write_mode)

    print("\n✅ Done")

if __name__ == "__main__":
    try:
        main()
    except Exception:
        print("❌ Fatal error:")
        traceback.print_exc()
        sys.exit(1)
