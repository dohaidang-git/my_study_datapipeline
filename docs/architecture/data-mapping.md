# Data Mapping

## Purpose

This document defines how source files in `data/raw/` map into bronze tables, how bronze tables map into silver tables, and how silver tables produce gold marts.

The goal is to make the pipeline implementation explicit before coding Spark jobs, so each dataset has a clear destination and each downstream table has a clear lineage.

## Source datasets in scope

The current workspace contains these raw source files under `data/raw/olist/`:

- `olist_orders_dataset.csv`
- `olist_order_items_dataset.csv`
- `olist_customers_dataset.csv`
- `olist_order_payments_dataset.csv`
- `olist_products_dataset.csv`
- `olist_sellers_dataset.csv`
- `olist_order_reviews_dataset.csv`
- `olist_geolocation_dataset.csv`
- `product_category_name_translation.csv`
- `online_retail_II.xlsx`

There is also a placeholder incremental source under:

- `data/raw/products_api/`

For the first MVP, the Olist CSV files are the primary input. `online_retail_II.xlsx` can be kept out of the first implementation unless a second source comparison is required later.

## Layer overview

The mapping follows the standard project layers:

- `raw`: source files exactly as landed
- `bronze`: source-aligned Hudi tables with light normalization
- `silver`: cleaned, conformed, and trusted business tables
- `gold`: business-facing marts for analytics and dashboards

## Raw to bronze mapping

### Core transaction files

| Raw file | Bronze table | Notes |
|---|---|---|
| `olist_orders_dataset.csv` | `orders_bronze` | Main order lifecycle table |
| `olist_order_items_dataset.csv` | `order_items_bronze` | One row per order item |
| `olist_order_payments_dataset.csv` | `payments_bronze` | Payment events per order |
| `olist_customers_dataset.csv` | `customers_bronze` | Customer identity and location columns |
| `olist_products_dataset.csv` | `products_bronze` | Product attributes and dimensions |

### Supporting dimension and enrichment files

| Raw file | Bronze table | Notes |
|---|---|---|
| `olist_sellers_dataset.csv` | `sellers_bronze` | Seller dimension |
| `olist_order_reviews_dataset.csv` | `reviews_bronze` | Review and satisfaction data |
| `olist_geolocation_dataset.csv` | `geolocation_bronze` | Zip-to-location lookup |
| `product_category_name_translation.csv` | `product_category_translation_bronze` | Portuguese to English category mapping |
| `data/raw/products_api/*.json` | `products_api_bronze` | Optional incremental product feed |

### Optional secondary source

| Raw file | Bronze table | Notes |
|---|---|---|
| `online_retail_II.xlsx` | `online_retail_bronze` | Optional second dataset, not required for first MVP |

## Bronze table responsibilities

Bronze tables stay close to source files and should only do lightweight handling:

- parse CSV or JSON correctly
- cast obvious data types
- preserve source columns
- add ingestion metadata

Recommended metadata columns:

- `_ingested_at`
- `_batch_id`
- `_source_file`
- `_source_system`

## Bronze to silver mapping

Silver tables are the trusted business layer. They clean and standardize the source-aligned bronze tables.

### Direct bronze to silver mappings

| Bronze table | Silver table | Main processing rules |
|---|---|---|
| `orders_bronze` | `orders_silver` | Parse timestamps, normalize `order_status`, deduplicate by `order_id` |
| `order_items_bronze` | `order_items_silver` | Cast `price` and `freight_value`, create stable item key |
| `payments_bronze` | `payments_silver` | Cast numeric fields, validate payment values |
| `customers_bronze` | `customers_silver` | Standardize city/state casing and zip prefixes |
| `products_bronze` | `products_silver_base` | Clean product attribute columns and handle nulls |
| `sellers_bronze` | `sellers_silver` | Standardize seller geography fields |
| `reviews_bronze` | `reviews_silver` | Parse review timestamps and review scores |
| `geolocation_bronze` | `geolocation_silver` | Standardize geolocation fields |
| `product_category_translation_bronze` | `product_category_translation_silver` | Clean translation lookup |
| `products_api_bronze` | `products_api_silver` | Normalize API product schema for merge logic |

### Derived silver mappings

Some silver tables are built by combining multiple bronze or silver inputs.

| Inputs | Silver output | Purpose |
|---|---|---|
| `products_silver_base` + `product_category_translation_silver` | `products_silver` | Enrich products with translated category names |
| `products_silver` + `products_api_silver` | `products_current_silver` | Merge optional API updates into current product state |
| `orders_silver` + `order_items_silver` | `order_facts_silver` | Base fact table for order-item level analytics |
| `order_facts_silver` + `payments_silver` | `order_payment_facts_silver` | Join order and payment behavior |
| `orders_silver` + `customers_silver` | `customer_orders_silver` | Customer-level order view |
| `order_items_silver` + `products_current_silver` + `sellers_silver` | `sales_enriched_silver` | Product and seller-enriched sales view |
| `orders_silver` + `reviews_silver` | `order_reviews_silver` | Link review outcomes to order lifecycle |

## Silver table responsibilities

Silver is where business correctness starts.

Typical silver processing includes:

- deduplication
- type normalization
- data quality checks
- lookup enrichment
- upsert merge logic
- entity conformance

At this layer, downstream consumers should not need to know the quirks of the raw files.

## Silver to gold mapping

Gold tables are analytics-serving marts with stable business meaning.

### Core gold marts

| Silver inputs | Gold table | Business meaning |
|---|---|---|
| `orders_silver` + `order_items_silver` + `payments_silver` | `daily_sales_gold` | Daily sales revenue, order count, item count |
| `sales_enriched_silver` | `category_sales_gold` | Revenue and volume by product category |
| `customer_orders_silver` + `payments_silver` | `customer_ltv_gold` | Lifetime value and order activity by customer |
| `orders_silver` | `delivery_performance_gold` | Delivery timeliness and status metrics |

### Extended gold marts

| Silver inputs | Gold table | Business meaning |
|---|---|---|
| `sales_enriched_silver` | `seller_performance_gold` | Sales and volume by seller |
| `order_reviews_silver` | `customer_satisfaction_gold` | Review score trends and satisfaction analysis |
| `payments_silver` | `payment_type_summary_gold` | Payment mix and installment behavior |

## MVP priority mapping

To keep the first implementation realistic, the pipeline should not build every possible table at once.

### Phase 1: must-have bronze tables

- `orders_bronze`
- `order_items_bronze`
- `customers_bronze`
- `payments_bronze`
- `products_bronze`

### Phase 2: must-have silver tables

- `orders_silver`
- `order_items_silver`
- `customers_silver`
- `payments_silver`
- `products_silver`

### Phase 3: must-have gold marts

- `daily_sales_gold`
- `category_sales_gold`
- `customer_ltv_gold`

These are enough to demonstrate:

- source ingestion
- Hudi upsert-ready storage
- cleaned business tables
- analytical marts for Trino and dashboards

## Recommended lineage for the first MVP

Use the following lineage as the initial implementation target:

```text
olist_orders_dataset.csv
    -> orders_bronze
    -> orders_silver

olist_order_items_dataset.csv
    -> order_items_bronze
    -> order_items_silver

olist_customers_dataset.csv
    -> customers_bronze
    -> customers_silver

olist_order_payments_dataset.csv
    -> payments_bronze
    -> payments_silver

olist_products_dataset.csv
    -> products_bronze
    -> products_silver_base

product_category_name_translation.csv
    -> product_category_translation_bronze
    -> product_category_translation_silver

products_silver_base + product_category_translation_silver
    -> products_silver

orders_silver + order_items_silver + payments_silver
    -> daily_sales_gold

order_items_silver + products_silver
    -> category_sales_gold

orders_silver + customers_silver + payments_silver
    -> customer_ltv_gold
```

## Suggested record keys by bronze and silver table

These are useful when implementing Hudi writers.

| Table | Suggested record key | Suggested precombine field |
|---|---|---|
| `orders_bronze` / `orders_silver` | `order_id` | `order_purchase_timestamp` or latest update timestamp |
| `order_items_bronze` / `order_items_silver` | `order_id` + `order_item_id` | `shipping_limit_date` |
| `payments_bronze` / `payments_silver` | `order_id` + `payment_sequential` | ingestion timestamp |
| `customers_bronze` / `customers_silver` | `customer_id` | ingestion timestamp |
| `products_bronze` / `products_silver` | `product_id` | ingestion timestamp or API update timestamp |
| `sellers_bronze` / `sellers_silver` | `seller_id` | ingestion timestamp |
| `reviews_bronze` / `reviews_silver` | `review_id` | review creation timestamp |

## Common transformations by entity

### Orders

Expected processing:
- parse order timestamps
- standardize order status
- handle null delivery dates
- derive order date fields for partitioning and reporting

### Order items

Expected processing:
- cast price fields to numeric
- build stable item-level record key
- retain seller and product references

### Customers

Expected processing:
- preserve customer identity keys
- normalize text fields
- keep geography usable for future regional analysis

### Payments

Expected processing:
- cast payment values and installment counts
- validate no negative amounts
- allow multiple payment rows per order

### Products

Expected processing:
- clean dimension-like attributes
- enrich translated category names
- optionally merge daily API updates

## Why this mapping matters

This mapping avoids several common implementation mistakes:

- loading one raw file into multiple inconsistent bronze targets
- skipping bronze and hard-coding transformations directly from raw to gold
- duplicating enrichment logic across multiple jobs
- creating gold tables without a stable silver layer

The lineage defined here should be treated as the contract for the first pipeline version.

## Summary

The first version of the project should center on this minimal but useful flow:

- Olist raw CSV files
- bronze Hudi tables per entity
- silver business-clean tables
- gold marts for daily sales, category sales, and customer LTV

That mapping is enough to support the MVP while leaving room for later extensions such as seller analytics, customer satisfaction, and incremental product updates.
