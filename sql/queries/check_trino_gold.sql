SHOW SCHEMAS FROM hive;

SHOW TABLES FROM hive.analytics;

SELECT COUNT(*) AS daily_sales_rows FROM hive.analytics.daily_sales_gold;
SELECT COUNT(*) AS category_sales_rows FROM hive.analytics.category_sales_gold;
SELECT COUNT(*) AS customer_ltv_rows FROM hive.analytics.customer_ltv_gold;

SELECT *
FROM hive.analytics.daily_sales_gold
ORDER BY order_date DESC
LIMIT 10;

SELECT *
FROM hive.analytics.category_sales_gold
ORDER BY order_purchase_date DESC, gross_item_value DESC
LIMIT 10;

SELECT *
FROM hive.analytics.customer_ltv_gold
ORDER BY lifetime_value DESC
LIMIT 10;
