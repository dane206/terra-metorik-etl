#!/usr/bin/env python3
"""
Terra Health Essentials — Metorik Revenue → BigQuery
=====================================================
Pulls daily revenue breakdown from the Metorik API.

Table produced:
  terra-analytics-prod.sources.metorik_revenue_daily

Run modes:
  python metorik_revenue.py --mode backfill    # 2022-08-20 → yesterday
  python metorik_revenue.py --mode incremental # last 7 days (default)

Requires:
  .env with METORIK_API_KEY=
"""

import os, sys, argparse, traceback, json, urllib.request, urllib.parse
from datetime import date, timedelta, datetime, timezone

from dotenv import load_dotenv
from google.cloud import bigquery

load_dotenv()

# ── Config ────────────────────────────────────────────────────────────────────
METORIK_API_KEY = os.getenv("METORIK_API_KEY")
EARLIEST_DATE   = "2022-08-20"
BQ_PROJECT      = "terra-analytics-prod"
BQ_DATASET      = "sources"
BQ_TABLE        = "metorik_revenue_daily"

bq = bigquery.Client(project=BQ_PROJECT)

# ── Schema ────────────────────────────────────────────────────────────────────
SF = bigquery.SchemaField
schema = [
    SF("date",          "DATE"),
    SF("gross",         "FLOAT64"),
    SF("discounts",     "FLOAT64"),
    SF("refunds",       "FLOAT64"),
    SF("refunds_count", "INTEGER"),
    SF("taxes",         "FLOAT64"),
    SF("shipping",      "FLOAT64"),
    SF("fees",          "FLOAT64"),
    SF("orders",        "INTEGER"),
    SF("items",         "INTEGER"),
    SF("net",           "FLOAT64"),
    SF("_loaded_at",    "TIMESTAMP"),
]

# ── API ───────────────────────────────────────────────────────────────────────
def fetch_revenue(start_date: str, end_date: str) -> dict:
    params = urllib.parse.urlencode({
        "start_date": start_date,
        "end_date":   end_date,
        "group_by":   "day",
    })
    url = f"https://app.metorik.com/api/v1/store/reports/revenue-by-date?{params}"
    req = urllib.request.Request(url, headers={
        "Authorization": f"Bearer {METORIK_API_KEY}",
        "Accept":        "application/json",
    })
    with urllib.request.urlopen(req) as r:
        return json.loads(r.read())

# ── Transform ─────────────────────────────────────────────────────────────────
def parse_rows(data: dict) -> list:
    loaded_at = datetime.now(timezone.utc).isoformat()
    rows = []
    for day in data["data"]:
        rows.append({
            "date":          day["date"],
            "gross":         float(day["gross"]),
            "discounts":     float(day["discounts"]),
            "refunds":       float(day["refunds"]),
            "refunds_count": int(day["refunds_count"]),
            "taxes":         float(day["taxes"]),
            "shipping":      float(day["shipping"]),
            "fees":          float(day["fees"]),
            "orders":        int(day["orders"]),
            "items":         int(day["items"]),
            "net":           float(day["net"]),
            "_loaded_at":    loaded_at,
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
        start_date = str(date.today() - timedelta(days=7))
        write_mode = bigquery.WriteDisposition.WRITE_APPEND
    else:
        start_date = EARLIEST_DATE
        write_mode = bigquery.WriteDisposition.WRITE_TRUNCATE

    mode_label = "backfill" if args.mode == "backfill" else "incremental"
    print(f"🚀 Metorik Revenue {mode_label}: {start_date} → {yesterday}")

    print(f"  Fetching {start_date} → {yesterday}...")
    data = fetch_revenue(start_date, yesterday)

    if data["meta"].get("results_limited"):
        print("  ⚠️  results_limited=true — data may be truncated")

    rows = parse_rows(data)
    print(f"  {len(rows):,} rows parsed")

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
