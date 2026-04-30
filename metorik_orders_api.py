#!/usr/bin/env python3
"""
Terra Health Essentials — Metorik Orders API → BigQuery
========================================================
Pulls orders from the Metorik API incrementally, using MAX(order_updated_at)
from BQ as the resume point. Uses a MERGE/upsert pattern to update existing
rows and insert new ones — no duplicates, no full reloads.

Table produced:
  terra-analytics-prod.sources.metorik_orders

Run modes:
  python metorik_orders_api.py --mode incremental  # default, resumes from BQ
  python metorik_orders_api.py --mode backfill     # full reload

Requires:
  config.ini with [metorik] api_key  OR  METORIK_API_KEY env var
"""

import os, sys, json, time, argparse, traceback, urllib.request, urllib.parse
from datetime import datetime, timedelta, timezone, date

import configparser
from google.cloud import bigquery

config = configparser.ConfigParser()
config.read(os.path.join(os.path.dirname(__file__), "config.ini"))

METORIK_API_KEY = os.environ.get("METORIK_API_KEY") or config.get("metorik", "api_key", fallback=None)
BQ_PROJECT      = os.environ.get("BQ_PROJECT", "terra-analytics-prod")
BQ_DATASET      = "sources"
BQ_TABLE        = "metorik_orders"
EARLIEST_DATE   = "2022-08-20"
BASE_URL        = "https://app.metorik.com/api/v1/store"
PER_PAGE        = 100
RATE_LIMIT_WAIT = 1.1

bq = bigquery.Client(project=BQ_PROJECT)

COLUMNS = [
    "order_id", "order_number", "order_created_at", "status", "fulfillment_status",
    "currency", "total", "total_shipping", "total_discount", "shipping_tax",
    "total_tax", "total_items", "total_refunds", "total_after_refunds", "net",
    "billing_address_first_name", "billing_address_last_name", "billing_address_company",
    "billing_address_address_1", "billing_address_address_2", "billing_address_city",
    "billing_address_state", "billing_address_postcode", "billing_address_country",
    "billing_address_email", "billing_address_phone",
    "shipping_address_first_name", "shipping_address_last_name", "shipping_address_company",
    "shipping_address_address_1", "shipping_address_address_2", "shipping_address_city",
    "shipping_address_state", "shipping_address_postcode", "shipping_address_country",
    "order_updated_at", "order_completed_at", "order_paid_at",
    "payment_method", "payment_method_title", "shipping_method", "shipping_method_title",
    "tags", "gift_card_used", "order_key", "customer_ip", "customer_user_agent",
    "customer_note", "customer_id", "customer_role", "customer_link_id",
    "sales_channel", "referring_site", "landing_path",
    "utm_campaign", "utm_medium", "utm_source", "utm_term", "utm_content", "utm_id",
    "coupon_codes", "coupon_amounts",
    "total_original", "total_shipping_original", "total_discount_original",
    "shipping_tax_original", "total_tax_original", "total_refunds_original",
    "total_after_refunds_original", "net_original",
    "cogs", "gross_profit", "product_cost", "transaction_cost", "shipping_cost", "extra_cost",
    "line_item_product_id", "line_item_name", "line_item_sku", "line_item_quantity",
    "line_item_meta", "line_item_id", "line_item_variation_id",
    "line_item_subtotal", "line_item_subtotal_tax", "line_item_total", "line_item_total_tax",
    "line_item_price", "line_item_cost",
    "line_item_subtotal_original", "line_item_subtotal_tax_original",
    "line_item_total_original", "line_item_total_tax_original", "line_item_price_original",
    "metorik_referer", "metorik_session_entry", "metorik_source_type",
    "metorik_utm_campaign", "metorik_utm_content", "metorik_utm_id",
    "metorik_utm_medium", "metorik_utm_source", "metorik_utm_term",
    "channel", "facebook_order_id", "shopify_checkout_id", "shopify_order_creator_id",
    "edge_delivery", "edge_delivery_control", "edge_delivery_enabled",
    "edge_delivery_experiment_id", "edge_delivery_session_id", "edge_delivery_visitor_id",
    "tiktok_order_number", "utm_campaign_2", "utm_content_2", "utm_data_source",
    "utm_id_2", "utm_medium_2", "utm_source_2", "utm_term_2", "utm_timestamp",
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
            f"SELECT MAX(order_updated_at) AS max_date FROM `{BQ_PROJECT}.{BQ_DATASET}.{BQ_TABLE}`"
        ).result())
        val = result[0].max_date if result else None
        if val:
            dt = datetime.strptime(val[:10], "%Y-%m-%d").date() - timedelta(days=1)
            print(f"  Resuming from: {dt}")
            return str(dt)
        return None
    except Exception:
        return None

def flatten_order(o):
    ba  = o.get("billing_address") or {}
    sa  = o.get("shipping_address") or {}
    utm = o.get("utm") or {}
    li  = (o.get("line_items") or [{}])[0]
    row = {
        "order_id": s(o.get("order_id") or o.get("id")),
        "order_number": s(o.get("order_number")),
        "order_created_at": s(o.get("order_created_at")),
        "status": s(o.get("status")),
        "fulfillment_status": s(o.get("fulfillment_status")),
        "currency": s(o.get("currency")),
        "total": s(o.get("total")),
        "total_shipping": s(o.get("total_shipping")),
        "total_discount": s(o.get("total_discount")),
        "shipping_tax": s(o.get("shipping_tax")),
        "total_tax": s(o.get("total_tax")),
        "total_items": s(o.get("total_items")),
        "total_refunds": s(o.get("total_refunds")),
        "total_after_refunds": s(o.get("total_after_refunds")),
        "net": s(o.get("net")),
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
        "shipping_address_first_name": s(sa.get("first_name")),
        "shipping_address_last_name": s(sa.get("last_name")),
        "shipping_address_company": s(sa.get("company")),
        "shipping_address_address_1": s(sa.get("address_1")),
        "shipping_address_address_2": s(sa.get("address_2")),
        "shipping_address_city": s(sa.get("city")),
        "shipping_address_state": s(sa.get("state")),
        "shipping_address_postcode": s(sa.get("postcode")),
        "shipping_address_country": s(sa.get("country")),
        "order_updated_at": s(o.get("order_updated_at")),
        "order_completed_at": s(o.get("order_completed_at")),
        "order_paid_at": s(o.get("order_paid_at")),
        "payment_method": s(o.get("payment_method")),
        "payment_method_title": s(o.get("payment_method_title")),
        "shipping_method": s(o.get("shipping_method")),
        "shipping_method_title": s(o.get("shipping_method_title")),
        "tags": s(o.get("tags")),
        "gift_card_used": s(o.get("gift_card_used")),
        "order_key": s(o.get("order_key")),
        "customer_ip": s(o.get("customer_ip")),
        "customer_user_agent": s(o.get("customer_user_agent")),
        "customer_note": s(o.get("customer_note")),
        "customer_id": s(o.get("customer_id")),
        "customer_role": s(o.get("customer_role")),
        "customer_link_id": s(o.get("customer_link_id")),
        "sales_channel": s(o.get("sales_channel")),
        "referring_site": s(o.get("referring_site")),
        "landing_path": s(o.get("landing_path")),
        "utm_campaign": s(utm.get("campaign") or o.get("utm_campaign")),
        "utm_medium": s(utm.get("medium") or o.get("utm_medium")),
        "utm_source": s(utm.get("source") or o.get("utm_source")),
        "utm_term": s(utm.get("term") or o.get("utm_term")),
        "utm_content": s(utm.get("content") or o.get("utm_content")),
        "utm_id": s(utm.get("id") or o.get("utm_id")),
        "coupon_codes": s(o.get("coupon_codes")),
        "coupon_amounts": s(o.get("coupon_amounts")),
        "total_original": s(o.get("total_original")),
        "total_shipping_original": s(o.get("total_shipping_original")),
        "total_discount_original": s(o.get("total_discount_original")),
        "shipping_tax_original": s(o.get("shipping_tax_original")),
        "total_tax_original": s(o.get("total_tax_original")),
        "total_refunds_original": s(o.get("total_refunds_original")),
        "total_after_refunds_original": s(o.get("total_after_refunds_original")),
        "net_original": s(o.get("net_original")),
        "cogs": s(o.get("cogs")),
        "gross_profit": s(o.get("gross_profit")),
        "product_cost": s(o.get("product_cost")),
        "transaction_cost": s(o.get("transaction_cost")),
        "shipping_cost": s(o.get("shipping_cost")),
        "extra_cost": s(o.get("extra_cost")),
        "line_item_product_id": s(li.get("product_id") or li.get("line_item_product_id")),
        "line_item_name": s(li.get("name") or li.get("line_item_name")),
        "line_item_sku": s(li.get("sku") or li.get("line_item_sku")),
        "line_item_quantity": s(li.get("quantity") or li.get("line_item_quantity")),
        "line_item_meta": s(li.get("meta") or li.get("line_item_meta")),
        "line_item_id": s(li.get("line_item_id") or li.get("id")),
        "line_item_variation_id": s(li.get("variation_id") or li.get("line_item_variation_id")),
        "line_item_subtotal": s(li.get("subtotal") or li.get("line_item_subtotal")),
        "line_item_subtotal_tax": s(li.get("subtotal_tax") or li.get("line_item_subtotal_tax")),
        "line_item_total": s(li.get("total") or li.get("line_item_total")),
        "line_item_total_tax": s(li.get("total_tax") or li.get("line_item_total_tax")),
        "line_item_price": s(li.get("price") or li.get("line_item_price")),
        "line_item_cost": s(li.get("cost") or li.get("line_item_cost")),
        "line_item_subtotal_original": s(li.get("subtotal_original") or li.get("line_item_subtotal_original")),
        "line_item_subtotal_tax_original": s(li.get("subtotal_tax_original") or li.get("line_item_subtotal_tax_original")),
        "line_item_total_original": s(li.get("total_original") or li.get("line_item_total_original")),
        "line_item_total_tax_original": s(li.get("total_tax_original") or li.get("line_item_total_tax_original")),
        "line_item_price_original": s(li.get("price_original") or li.get("line_item_price_original")),
        "metorik_referer": s(o.get("metorik_referer")),
        "metorik_session_entry": s(o.get("metorik_session_entry")),
        "metorik_source_type": s(o.get("metorik_source_type")),
        "metorik_utm_campaign": s(o.get("metorik_utm_campaign")),
        "metorik_utm_content": s(o.get("metorik_utm_content")),
        "metorik_utm_id": s(o.get("metorik_utm_id")),
        "metorik_utm_medium": s(o.get("metorik_utm_medium")),
        "metorik_utm_source": s(o.get("metorik_utm_source")),
        "metorik_utm_term": s(o.get("metorik_utm_term")),
        "channel": s(o.get("channel")),
        "facebook_order_id": s(o.get("facebook_order_id")),
        "shopify_checkout_id": s(o.get("shopify_checkout_id")),
        "shopify_order_creator_id": s(o.get("shopify_order_creator_id")),
        "edge_delivery": s(o.get("edge_delivery")),
        "edge_delivery_control": s(o.get("edge_delivery_control")),
        "edge_delivery_enabled": s(o.get("edge_delivery_enabled")),
        "edge_delivery_experiment_id": s(o.get("edge_delivery_experiment_id")),
        "edge_delivery_session_id": s(o.get("edge_delivery_session_id")),
        "edge_delivery_visitor_id": s(o.get("edge_delivery_visitor_id")),
        "tiktok_order_number": s(o.get("tiktok_order_number")),
        "utm_campaign_2": s(o.get("utm_campaign_2")),
        "utm_content_2": s(o.get("utm_content_2")),
        "utm_data_source": s(o.get("utm_data_source")),
        "utm_id_2": s(o.get("utm_id_2")),
        "utm_medium_2": s(o.get("utm_medium_2")),
        "utm_source_2": s(o.get("utm_source_2")),
        "utm_term_2": s(o.get("utm_term_2")),
        "utm_timestamp": s(o.get("utm_timestamp")),
    }
    return {k: v for k, v in row.items() if v is not None}

def fetch_orders(updated_after=None):
    rows, page = [], 1
    params = {"per_page": PER_PAGE}
    if updated_after:
        params["updated_after"] = updated_after
    while True:
        params["page"] = page
        print(f"\r  Fetching page {page} ({len(rows):,} orders)...", end="", flush=True)
        data = api_get("/orders", params)
        batch = data.get("data") or []
        if not batch:
            break
        for o in batch:
            rows.append(flatten_order(o))
        pagination = data.get("pagination") or data.get("meta") or {}
        has_more = pagination.get("has_more_pages") or pagination.get("last_page", 1) > page
        if not has_more:
            break
        page += 1
        time.sleep(RATE_LIMIT_WAIT)
    print(f"\n  {len(rows):,} orders fetched")
    return rows

def merge_to_bq(rows):
    if not rows:
        print("  ⚠️  No rows to merge")
        return
    staging = f"{BQ_PROJECT}.{BQ_DATASET}._metorik_orders_staging"
    main    = f"{BQ_PROJECT}.{BQ_DATASET}.{BQ_TABLE}"
    print(f"  Loading {len(rows):,} rows to staging...")
    bq.load_table_from_json(rows, staging, job_config=bigquery.LoadJobConfig(
        schema=SCHEMA, write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
        ignore_unknown_values=True)).result()
    set_clause    = ",\n    ".join(f"T.`{c}` = S.`{c}`" for c in COLUMNS if c != "order_id")
    insert_cols   = ", ".join(f"`{c}`" for c in COLUMNS)
    insert_values = ", ".join(f"S.`{c}`" for c in COLUMNS)
    bq.query(f"""
    MERGE `{main}` T USING `{staging}` S ON T.order_id = S.order_id
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
    print(f"🚀 Metorik Orders API → {BQ_PROJECT}.{BQ_DATASET}.{BQ_TABLE} [{args.mode}]")
    updated_after = get_resume_date() if args.mode == "incremental" else None
    if args.mode == "backfill":
        print("  Backfill: all orders")
    rows = fetch_orders(updated_after)
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
