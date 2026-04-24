#!/usr/bin/env python3
"""
Terra Health Essentials — Metorik + Shopify Export Loader → BigQuery
====================================================================
Loads CSV/ZIP exports into terra-analytics-prod.sources.*

All fields loaded as STRING — casting handled in staging layer.

Usage:
  python metorik_load.py --data-dir ~/projects/data/metorik
  python metorik_load.py --data-dir ~/projects/data/metorik --skip metorik_carts metorik_customers
"""

import os, sys, glob, zipfile, argparse, traceback
import pandas as pd
from google.cloud import bigquery

BQ_PROJECT = "terra-analytics-prod"
BQ_DATASET = "sources"

bq = bigquery.Client(project=BQ_PROJECT)

# ── Helpers ───────────────────────────────────────────────────────────────────
def slugify(col):
    return (col.lower()
               .replace(" ", "_")
               .replace(".", "_")
               .replace("/", "_")
               .replace("-", "_")
               .replace("(", "")
               .replace(")", "")
               .replace("__", "_")
               .strip("_"))

def dedup_columns(columns):
    seen = {}
    result = []
    for col in columns:
        if col in seen:
            seen[col] += 1
            result.append(f"{col}_{seen[col]}")
        else:
            seen[col] = 1
            result.append(col)
    return result

def load_csv(path):
    df = pd.read_csv(path, low_memory=False, dtype=str)
    df.columns = dedup_columns([slugify(c) for c in df.columns])
    return df

def load_zip(path):
    dfs = []
    with zipfile.ZipFile(path) as z:
        for name in z.namelist():
            if name.endswith(".csv"):
                with z.open(name) as f:
                    df = pd.read_csv(f, low_memory=False, dtype=str)
                    df.columns = dedup_columns([slugify(c) for c in df.columns])
                    dfs.append(df)
    return pd.concat(dfs, ignore_index=True) if dfs else pd.DataFrame()

def to_bq(df, table_name, skip):
    if table_name in skip:
        print(f"  ⏭️  {table_name} — skipped")
        return
    if df.empty:
        print(f"  ⚠️  {table_name} — no data, skipping")
        return
    df = df.dropna(axis=1, how="all")
    schema = [bigquery.SchemaField(col, "STRING") for col in df.columns]
    rows = [{k: v for k, v in row.items() if pd.notna(v) and v != ""} for _, row in df.iterrows()]
    table_id = f"{BQ_PROJECT}.{BQ_DATASET}.{table_name}"
    job = bq.load_table_from_json(rows, table_id, job_config=bigquery.LoadJobConfig(
        schema=schema, write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE))
    job.result()
    tbl = bq.get_table(table_id)
    print(f"  ✅ {table_id} — {tbl.num_rows:,} rows, {len(schema)} columns")

def find_latest(data_dir, pattern):
    matches = glob.glob(os.path.join(data_dir, pattern))
    return max(matches, key=os.path.getmtime) if matches else None

# ── Loaders ───────────────────────────────────────────────────────────────────
def run(data_dir, skip):
    def load(pattern, table, is_zip=False):
        if table in skip:
            print(f"  ⏭️  {table} — skipped")
            return
        f = find_latest(data_dir, pattern)
        if not f:
            print(f"  ⚠️  No file found for {table}")
            return
        print(f"  Loading {os.path.basename(f)}")
        df = load_zip(f) if is_zip else load_csv(f)
        to_bq(df, table, skip)

    def load_multi_zip(pattern, table):
        if table in skip:
            print(f"  ⏭️  {table} — skipped")
            return
        zips = sorted(glob.glob(os.path.join(data_dir, pattern)))
        if not zips:
            print(f"  ⚠️  No files found for {table}")
            return
        dfs = []
        for z in zips:
            print(f"  Loading {os.path.basename(z)}")
            dfs.append(load_zip(z))
        to_bq(pd.concat(dfs, ignore_index=True), table, skip)

    print("📦 Metorik exports...")
    load("carts-*.csv",              "metorik_carts")
    load("customers-*.csv",          "metorik_customers")
    load("orders-*.csv",             "metorik_orders")
    load("weekly-categories-*.csv",  "metorik_categories")
    load("weekly-coupons-*.csv",     "metorik_coupons")
    load("weekly-products-*.csv",    "metorik_products")
    load("weekly-refunds-*.csv",     "metorik_refunds")
    load("weekly-variations-*.csv",  "metorik_variations")

    print("\n🛍️  Shopify native exports...")
    load("shopify_products_export_*.csv",     "shopify_products_enriched")
    load("shopify_discounts_export_*.zip",    "shopify_discount_catalog", is_zip=True)
    load_multi_zip("shopify_transactions_export_*.zip", "shopify_transactions")

# ── Main ──────────────────────────────────────────────────────────────────────
def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--data-dir", required=True)
    parser.add_argument("--skip", nargs="*", default=[], help="Table names to skip")
    args = parser.parse_args()

    data_dir = os.path.expanduser(args.data_dir)
    if not os.path.isdir(data_dir):
        print(f"❌ Directory not found: {data_dir}")
        sys.exit(1)

    skip = set(args.skip)
    print(f"🚀 Metorik/Shopify Export Loader → {BQ_PROJECT}.{BQ_DATASET}.*")
    print(f"   data-dir: {data_dir}")
    if skip:
        print(f"   skipping: {', '.join(skip)}")
    print()

    run(data_dir, skip)
    print("\n✅ Done")

if __name__ == "__main__":
    try:
        main()
    except Exception:
        print("❌ Fatal error:")
        traceback.print_exc()
        sys.exit(1)
