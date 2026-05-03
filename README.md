# terra-metorik-etl

Pulls Metorik API data and loads Metorik/Shopify CSV exports → `terra-analytics-prod.sources.*`

## Tables produced

### API pulls

| Script | Table | Granularity | Mode |
|---|---|---|---|
| `metorik_ad_spend.py` | `metorik_ad_spend_daily` | platform × day | incremental / backfill |
| `metorik_profit.py` | `metorik_profit_daily` | day | incremental / backfill |
| `metorik_revenue.py` | `metorik_revenue_daily` | day | incremental / backfill |
| `metorik_sources_utms.py` | `metorik_sources_utms` | utm combo × period | full reload |
| `metorik_orders_api.py` | `metorik_orders` | one row per order | incremental MERGE / backfill |
| `metorik_customers_api.py` | `metorik_customers` | one row per customer | incremental MERGE / backfill |

### CSV/ZIP exports (manual)

| File pattern | Table |
|---|---|
| `carts-*.csv` | `metorik_carts` |
| `customers-*.csv` | `metorik_customers` |
| `orders-*.csv` | `metorik_orders` |
| `weekly-categories-*.csv` | `metorik_categories` |
| `weekly-coupons-*.csv` | `metorik_coupons` |
| `weekly-products-*.csv` | `metorik_products` |
| `weekly-refunds-*.csv` | `metorik_refunds` |
| `weekly-variations-*.csv` | `metorik_variations` |
| `shopify_products_export_*.csv` | `shopify_products_enriched` |
| `shopify_discounts_export_*.zip` | `shopify_discount_catalog` |
| `shopify_transactions_export_*.zip` | `shopify_transactions` |

## Setup

```bash
python3 -m venv venv
source venv/bin/activate
pip install -r requirements.txt
```

Add credentials to `config.ini`:

```ini
[metorik]
api_key = YOUR_METORIK_API_KEY
```

## Usage

```bash
# API pulls
python metorik_ad_spend.py --mode incremental      # last 7 days, APPEND
python metorik_ad_spend.py --mode backfill          # 2022-08-20 → yesterday, TRUNCATE

python metorik_profit.py --mode incremental
python metorik_profit.py --mode backfill

python metorik_revenue.py --mode incremental
python metorik_revenue.py --mode backfill

python metorik_sources_utms.py                      # always full TRUNCATE + reload

# Order and customer API pulls (MERGE/upsert)
python metorik_orders_api.py --mode incremental
python metorik_orders_api.py --mode backfill
python metorik_customers_api.py --mode incremental
python metorik_customers_api.py --mode backfill

# Run all incremental scripts in sequence (Cloud Run entrypoint)
python run_incremental.py

# CSV/ZIP export loader
python metorik_load.py --data-dir ~/projects/data/metorik
python metorik_load.py --data-dir ~/projects/data/metorik --skip metorik_carts metorik_customers
```

## Auth

Metorik API uses a Bearer token from `config.ini`. BigQuery uses Application Default Credentials (ADC).
