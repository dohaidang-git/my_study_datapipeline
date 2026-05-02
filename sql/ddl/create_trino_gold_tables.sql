CREATE TABLE IF NOT EXISTS hive.analytics.daily_sales_gold (
    order_count BIGINT,
    item_count BIGINT,
    gross_item_value DECIMAL(32, 2),
    gross_freight_value DECIMAL(32, 2),
    payment_value DECIMAL(32, 2),
    avg_order_payment_value DECIMAL(26, 6),
    gold_key VARCHAR,
    order_date DATE
)
WITH (
    external_location = 's3://lakehouse/gold/daily_sales_gold',
    format = 'PARQUET',
    partitioned_by = ARRAY['order_date']
);

CREATE TABLE IF NOT EXISTS hive.analytics.category_sales_gold (
    category_name VARCHAR,
    order_count BIGINT,
    item_count BIGINT,
    gross_item_value DECIMAL(22, 2),
    gross_freight_value DECIMAL(22, 2),
    gold_key VARCHAR,
    order_purchase_date DATE
)
WITH (
    external_location = 's3://lakehouse/gold/category_sales_gold',
    format = 'PARQUET',
    partitioned_by = ARRAY['order_purchase_date']
);

CREATE TABLE IF NOT EXISTS hive.analytics.customer_ltv_gold (
    customer_id VARCHAR,
    customer_unique_id VARCHAR,
    customer_city VARCHAR,
    customer_state VARCHAR,
    order_count BIGINT,
    lifetime_value DECIMAL(32, 2),
    first_order_date DATE,
    last_order_date DATE,
    customer_lifespan_days INTEGER,
    avg_order_value DECIMAL(38, 8),
    ltv_rank INTEGER
)
WITH (
    external_location = 's3://lakehouse/gold/customer_ltv_gold',
    format = 'PARQUET'
);
