# terra-metorik-etl

Loads Metorik and Shopify native CSV exports into `terra-analytics-prod.sources.*`.

## Tables produced

| File | BQ Table |
|---|---|
| `carts-*.csv` | `terra-analytics-prod.sources.metorik_carts` |
| `customers-*.csv` | `terra-analytics-prod.sources.metorik_customers` |
| `orders-*.csv` | `terra-analytics-prod.sources.metorik_orders` |
| `weekly-categories-*.csv` | `terra-analytics-prod.sources.metorik_categories` |
| `weekly-coupons-*.csv` | `terra-analytics-prod.sources.metorik_coupons` |
| `weekly-products-*.csv` | `terra-analytics-prod.sources.metorik_products` |
| `weekly-refunds-*.csv` | `terra-analytics-prod.sources.metorik_refunds` |
| `weekly-variations-*.csv` | `terra-analytics-prod.sources.metorik_variations` |
| `shopify_products_export_1.csv` | `terra-analytics-prod.sources.shopify_products_enriched` |
| `shopify_discounts_export_1.zip` | `terra-analytics-prod.sources.shopify_discount_catalog` |
| `shopify_transactions_export_*.zip` | `terra-analytics-prod.sources.shopify_transactions` |

## Usage

```bash
python metorik_load.py --data-dir ~/projects/data/metorik
```

## Setup

```bash
python3 -m venv venv
source venv/bin/activate
pip install -r requirements.txt
```
