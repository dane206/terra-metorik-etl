#!/usr/bin/env python3
"""
Terra Health Essentials — Metorik UTM Sources → BigQuery
=========================================================
Pulls UTM performance from the Metorik API, chunked by quarter.
Any quarter that hits the 1500-row cap is automatically re-fetched
monthly to maximize coverage.

Table produced:
  terra-analytics-prod.sources.metorik_sources_utms

Always does a full truncate + reload.

Run:
  python metorik_sources_utms.py

Requires:
  config.ini with [metorik] api_key
"""

import os, sys, traceback, json, urllib.request, urllib.parse
from datetime import date, timedelta, datetime, timezone
import calendar

import configparser
from google.cloud import bigquery

config = configparser.ConfigParser()
config.read(os.path.join(os.path.dirname(__file__), "config.ini"))

# ── Config ────────────────────────────────────────────────────────────────────
METORIK_API_KEY = config["metorik"]["api_key"]
EARLIEST_DATE   = "2022-08-20"
BQ_PROJECT      = "terra-analytics-prod"
BQ_DATASET      = "sources"
BQ_TABLE        = "metorik_sources_utms"
CAP             = 1500

bq = bigquery.Client(project=BQ_PROJECT)

# ── Schema ────────────────────────────────────────────────────────────────────
SF = bigquery.SchemaField
schema = [
    SF("period_start",          "DATE"),
    SF("period_end",            "DATE"),
    SF("utm_source",            "STRING"),
    SF("utm_medium",            "STRING"),
    SF("utm_campaign",          "STRING"),
    SF("source_combined",       "STRING"),
    SF("count",                 "INTEGER"),
    SF("total",                 "FLOAT64"),
    SF("net",                   "FLOAT64"),
    SF("average",               "FLOAT64"),
    SF("average_net",           "FLOAT64"),
    SF("total_refunds",         "FLOAT64"),
    SF("total_taxes",           "FLOAT64"),
    SF("total_shipping",        "FLOAT64"),
    SF("total_fees",            "FLOAT64"),
    SF("total_after_refunds",   "FLOAT64"),
    SF("_loaded_at",            "TIMESTAMP"),
]

# ── Date chunks ───────────────────────────────────────────────────────────────
QUARTER_STARTS = [1, 4, 7, 10]

def quarter_chunks(earliest: str, end: str):
    start = datetime.strptime(earliest, "%Y-%m-%d").date()
    stop  = datetime.strptime(end, "%Y-%m-%d").date()
    chunks = []
    year = start.year
    while True:
        for q_start_month in QUARTER_STARTS:
            q_start = date(year, q_start_month, 1)
            q_end_month = q_start_month + 2
            if q_end_month == 12:
                q_end = date(year, 12, 31)
            else:
                q_end = date(year, q_end_month + 1, 1) - timedelta(days=1)
            if q_end < start:
                continue
            if q_start > stop:
                return chunks
            chunks.append((str(max(q_start, start)), str(min(q_end, stop))))
        year += 1

def month_chunks(q_start: str, q_end: str):
    start = datetime.strptime(q_start, "%Y-%m-%d").date()
    stop  = datetime.strptime(q_end, "%Y-%m-%d").date()
    chunks = []
    current = date(start.year, start.month, 1)
    while current <= stop:
        last_day = calendar.monthrange(current.year, current.month)[1]
        m_end = date(current.year, current.month, last_day)
        chunks.append((str(max(current, start)), str(min(m_end, stop))))
        # advance to next month
        if current.month == 12:
            current = date(current.year + 1, 1, 1)
        else:
            current = date(current.year, current.month + 1, 1)
    return chunks

# ── API ───────────────────────────────────────────────────────────────────────
def fetch_utms(start_date: str, end_date: str) -> dict:
    params = urllib.parse.urlencode({
        "start_date":  start_date,
        "end_date":    end_date,
        "source_type": "utm_source,utm_medium,utm_campaign",
    })
    url = f"https://app.metorik.com/api/v1/store/reports/sources-utms?{params}"
    req = urllib.request.Request(url, headers={
        "Authorization": f"Bearer {METORIK_API_KEY}",
        "Accept":        "application/json",
    })
    with urllib.request.urlopen(req) as r:
        return json.loads(r.read())

# ── Transform ─────────────────────────────────────────────────────────────────
def parse_rows(data: dict, start_date: str, end_date: str, loaded_at: str) -> list:
    rows = []
    for row in data["data"]:
        rows.append({
            "period_start":        start_date,
            "period_end":          end_date,
            "utm_source":          str(row["utm_source"]) if row["utm_source"] is not None else None,
            "utm_medium":          str(row["utm_medium"]) if row["utm_medium"] is not None else None,
            "utm_campaign":        str(row["utm_campaign"]) if row["utm_campaign"] is not None else None,
            "source_combined":     str(row.get("source_combined")),
            "count":               int(row.get("count", 0)),
            "total":               float(row.get("total", 0)),
            "net":                 float(row.get("net", 0)),
            "average":             float(row.get("average", 0)),
            "average_net":         float(row.get("average_net", 0)),
            "total_refunds":       float(row.get("total_refunds", 0)),
            "total_taxes":         float(row.get("total_taxes", 0)),
            "total_shipping":      float(row.get("total_shipping", 0)),
            "total_fees":          float(row.get("total_fees", 0)),
            "total_after_refunds": float(row.get("total_after_refunds", 0)),
            "_loaded_at":          loaded_at,
        })
    return rows

# ── Load ──────────────────────────────────────────────────────────────────────
def load_to_bq(rows: list):
    if not rows:
        print("  ⚠️  No rows to load")
        return
    table_id = f"{BQ_PROJECT}.{BQ_DATASET}.{BQ_TABLE}"
    job = bq.load_table_from_json(rows, table_id, job_config=bigquery.LoadJobConfig(
        schema=schema,
        write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
        ignore_unknown_values=True,
    ))
    job.result()
    print(f"  ✅ {table_id} — {bq.get_table(table_id).num_rows:,} rows total")

# ── Fetch with auto monthly fallback ─────────────────────────────────────────
def fetch_chunk(chunk_start: str, chunk_end: str, loaded_at: str) -> list:
    data = fetch_utms(chunk_start, chunk_end)
    rows = parse_rows(data, chunk_start, chunk_end, loaded_at)

    if data["meta"].get("results_limited"):
        print(f"    ⚠️  Cap hit — switching to monthly chunks for {chunk_start[:7]} → {chunk_end[:7]}")
        rows = []
        for m_start, m_end in month_chunks(chunk_start, chunk_end):
            print(f"    Fetching {m_start} → {m_end}...")
            m_data = fetch_utms(m_start, m_end)
            m_rows = parse_rows(m_data, m_start, m_end, loaded_at)
            if m_data["meta"].get("results_limited"):
                print(f"    ⚠️  Still capped at monthly level for {m_start[:7]} — accepting 1500")
            print(f"    {len(m_rows):,} combos")
            rows.extend(m_rows)
    else:
        print(f"  {len(rows):,} combos")

    return rows

# ── Main ──────────────────────────────────────────────────────────────────────
def main():
    if not METORIK_API_KEY:
        print("❌ METORIK_API_KEY not set in .env")
        sys.exit(1)

    yesterday = str(date.today() - timedelta(days=1))
    loaded_at = datetime.now(timezone.utc).isoformat()
    chunks    = quarter_chunks(EARLIEST_DATE, yesterday)

    print(f"🚀 Metorik UTM Sources: {EARLIEST_DATE} → {yesterday} ({len(chunks)} quarter chunks)")

    all_rows = []
    for chunk_start, chunk_end in chunks:
        print(f"  Fetching {chunk_start} → {chunk_end}...")
        rows = fetch_chunk(chunk_start, chunk_end, loaded_at)
        all_rows.extend(rows)

    print(f"\n  {len(all_rows):,} total rows across all chunks")
    print("\n💾 Loading to BigQuery...")
    load_to_bq(all_rows)

    print("\n✅ Done")

if __name__ == "__main__":
    try:
        main()
    except Exception:
        print("❌ Fatal error:")
        traceback.print_exc()
        sys.exit(1)
