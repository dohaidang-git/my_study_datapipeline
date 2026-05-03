# Freshness And Reconciliation Checks

## Mục đích

Lớp kiểm tra này nằm sau `data quality checks`.

Nếu `quality checks` trả lời:

- dữ liệu trong từng bảng có hợp lý không

thì `freshness and reconciliation checks` trả lời:

- các bảng có còn khớp với nhau không
- metric giữa các tầng có bị lệch sau transform hay không
- ngày dữ liệu cuối cùng ở các bảng gold có còn đồng bộ với silver hay không

## File thực thi

- [pipelines/tools/run_freshness_reconciliation_checks.py](/home/dohaidang/bigdata_hudi/pipelines/tools/run_freshness_reconciliation_checks.py:1)

## Những gì đang được kiểm tra

### Freshness alignment

- `orders_silver.max(order_purchase_date)` phải bằng:
  - `daily_sales_gold.max(order_date)`
  - `customer_ltv_gold.max(last_order_date)`
- `orders có item`.max(`order_purchase_date`) phải bằng:
  - `category_sales_gold.max(order_purchase_date)`
- `orders_silver.min(order_purchase_date)` phải bằng `daily_sales_gold.min(order_date)`

Lưu ý:

- `category_sales_gold` được build từ `order_items_silver`
- vì vậy nó chỉ bao phủ các order thực sự có item
- trong dữ liệu Olist hiện tại có một nhóm order `canceled` hoặc `unavailable` không có item
- do đó ngày lớn nhất của `category_sales_gold` hợp lý là nhỏ hơn ngày lớn nhất của toàn bộ `orders_silver`

## Reconciliation checks

### Order count reconciliation

- tổng `daily_sales_gold.order_count` phải bằng số `order_id` distinct ở `orders_silver`
- tổng `customer_ltv_gold.order_count` phải bằng số `order_id` distinct ở `orders_silver`

### Item count reconciliation

- tổng `category_sales_gold.item_count` phải bằng số `order_item_key` distinct ở `order_items_silver`

### Payment value reconciliation

- tổng `payments_silver.payment_value` phải khớp với:
  - tổng `daily_sales_gold.payment_value`
  - tổng `customer_ltv_gold.lifetime_value`

### Gross item and freight reconciliation

- tổng `order_items_silver.price` phải khớp với:
  - tổng `daily_sales_gold.gross_item_value`
  - tổng `category_sales_gold.gross_item_value`
- tổng `order_items_silver.freight_value` phải khớp với:
  - tổng `daily_sales_gold.gross_freight_value`
  - tổng `category_sales_gold.gross_freight_value`

## Cách chạy thủ công

```bash
bash scripts/spark_submit_container.sh pipelines/tools/run_freshness_reconciliation_checks.py
```

## Cách chạy qua Airflow

Trong DAG `hudi_full_pipeline`, task chạy là:

- `freshness_reconciliation_hudi`

Nó chạy sau:

- `quality_checks_hudi`

và trước:

- `verify_trino_gold`

## Vì sao lớp kiểm tra này quan trọng

Có nhiều lỗi mà:

- row count verify không bắt được
- quality checks theo từng bảng cũng không bắt được

Ví dụ:

- join sai làm metric doanh thu bị phóng đại
- grain gold sai nhưng vẫn không duplicate theo key hiện có
- một bảng gold bị build thiếu ngày cuối

Những lỗi đó thường chỉ lộ ra khi so sánh chéo giữa các bảng.
