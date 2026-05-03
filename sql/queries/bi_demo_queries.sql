-- BI demo queries for hive.analytics gold tables.
-- Use these queries in:
-- 1. Trino CLI
-- 2. Metabase SQL editor
-- 3. Demo report export scripts

-- 1. Daily sales trend
SELECT
    order_date,
    order_count,
    item_count,
    payment_value,
    avg_order_payment_value
FROM hive.analytics.daily_sales_gold
ORDER BY order_date;

-- 2. Monthly sales summary
SELECT
    date_trunc('month', order_date) AS sales_month,
    SUM(order_count) AS order_count,
    SUM(item_count) AS item_count,
    SUM(payment_value) AS total_payment_value,
    AVG(avg_order_payment_value) AS avg_daily_order_payment_value
FROM hive.analytics.daily_sales_gold
GROUP BY 1
ORDER BY 1;

-- 3. Top 15 categories by gross item value
SELECT
    category_name,
    SUM(order_count) AS order_count,
    SUM(item_count) AS item_count,
    ROUND(SUM(gross_item_value), 2) AS gross_item_value,
    ROUND(SUM(gross_freight_value), 2) AS gross_freight_value
FROM hive.analytics.category_sales_gold
GROUP BY 1
ORDER BY gross_item_value DESC
LIMIT 15;

-- 4. Top 15 categories by order count
SELECT
    category_name,
    SUM(order_count) AS order_count,
    ROUND(SUM(gross_item_value), 2) AS gross_item_value
FROM hive.analytics.category_sales_gold
GROUP BY 1
ORDER BY order_count DESC
LIMIT 15;

-- 5. Top 20 customers by lifetime value
SELECT
    customer_id,
    customer_unique_id,
    customer_city,
    customer_state,
    order_count,
    ROUND(lifetime_value, 2) AS lifetime_value,
    ROUND(avg_order_value, 2) AS avg_order_value,
    first_order_date,
    last_order_date,
    ltv_rank
FROM hive.analytics.customer_ltv_gold
ORDER BY lifetime_value DESC, customer_id
LIMIT 20;

-- 6. Top 15 states by lifetime value
SELECT
    customer_state,
    COUNT(*) AS customer_count,
    SUM(order_count) AS order_count,
    ROUND(SUM(lifetime_value), 2) AS lifetime_value,
    ROUND(AVG(avg_order_value), 2) AS avg_order_value
FROM hive.analytics.customer_ltv_gold
GROUP BY 1
ORDER BY lifetime_value DESC
LIMIT 15;

-- 7. Monthly category leaders
WITH ranked_categories AS (
    SELECT
        date_trunc('month', order_purchase_date) AS sales_month,
        category_name,
        ROUND(SUM(gross_item_value), 2) AS gross_item_value,
        ROW_NUMBER() OVER (
            PARTITION BY date_trunc('month', order_purchase_date)
            ORDER BY SUM(gross_item_value) DESC, category_name
        ) AS category_rank
    FROM hive.analytics.category_sales_gold
    GROUP BY 1, 2
)
SELECT
    sales_month,
    category_name,
    gross_item_value
FROM ranked_categories
WHERE category_rank <= 3
ORDER BY sales_month, category_rank;

-- 8. Daily payment vs freight efficiency
SELECT
    order_date,
    payment_value,
    gross_item_value,
    gross_freight_value,
    ROUND(payment_value - gross_freight_value, 2) AS payment_minus_freight,
    ROUND(gross_freight_value / NULLIF(payment_value, 0), 4) AS freight_ratio
FROM hive.analytics.daily_sales_gold
ORDER BY order_date;
