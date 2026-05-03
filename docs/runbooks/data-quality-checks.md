# Data Quality Checks

## Mục đích

File này mô tả lớp kiểm tra chất lượng dữ liệu hiện có trong project.

Nó khác với `verify_hudi_pipeline.py`:

- `verify_hudi_pipeline.py` kiểm tra số dòng và sự tồn tại của bảng
- `run_data_quality_checks.py` kiểm tra tính đúng đắn của dữ liệu trong bảng

Nói ngắn:

- `verify` trả lời: bảng có được build đủ không
- `quality checks` trả lời: dữ liệu trong bảng có hợp lý không

## File thực thi

Script chính:

- [pipelines/tools/run_data_quality_checks.py](/home/dohaidang/bigdata_hudi/pipelines/tools/run_data_quality_checks.py:1)

Script này đọc trực tiếp các bảng `Hudi` ở tầng `silver` và `gold`.

## Các loại kiểm tra hiện tại

### 1. Not null checks

Kiểm tra các cột business key hoặc cột bắt buộc không bị `null`.

Ví dụ:

- `orders_silver.order_id`
- `payments_silver.payment_key`
- `customer_ltv_gold.customer_id`

## 2. Uniqueness checks

Kiểm tra grain chính của bảng có bị duplicate không.

Ví dụ:

- `orders_silver` phải unique theo `order_id`
- `geolocation_silver` phải unique theo `geolocation_key`
- `daily_sales_gold` phải unique theo `order_date`
- `category_sales_gold` phải unique theo `order_purchase_date + category_name`

## 3. Range checks

Kiểm tra giá trị numeric có nằm trong khoảng hợp lý không.

Ví dụ:

- `reviews_silver.review_score` phải trong `[1, 5]`
- `payments_silver.payment_value >= 0`
- `customer_ltv_gold.lifetime_value >= 0`

## 4. Format and normalization checks

Kiểm tra một số cột đã được chuẩn hóa như mong muốn.

Ví dụ:

- `orders_silver.order_status` phải là lowercase sau khi trim
- `customers_silver.customer_state` và `geolocation_silver.geolocation_state` phải có độ dài `2`

## 5. Gold grain checks

Đây là nhóm kiểm tra quan trọng nhất cho tầng `gold`.

Vì `gold` là tầng phục vụ phân tích, nên phải đảm bảo:

- grain đúng
- metric không âm
- không bị nhân dòng do join sai

Ví dụ:

- `daily_sales_gold` chỉ có 1 dòng cho mỗi ngày
- `category_sales_gold` chỉ có 1 dòng cho mỗi `ngày + category`
- `customer_ltv_gold` chỉ có 1 dòng cho mỗi `customer`

## Cách chạy thủ công

Chạy trực tiếp bằng wrapper Spark container:

```bash
bash scripts/spark_submit_container.sh pipelines/tools/run_data_quality_checks.py
```

## Cách chạy qua Airflow

DAG:

- `hudi_full_pipeline`

Task:

- `quality_checks_hudi`

Task này chạy sau:

- `verify_hudi_pipeline`

và trước:

- `verify_trino_gold`

Điều này có nghĩa là:

1. pipeline phải build xong `gold`
2. verify row count phải pass
3. quality checks phải pass
4. sau đó mới sang bước query smoke trên `Trino`

## Khi nào nên mở rộng quality checks

Nên thêm rule mới khi:

- thêm bảng mới ở `silver` hoặc `gold`
- thay đổi grain của bảng
- thay đổi business logic aggregate
- gặp bug dữ liệu mà row count verify không phát hiện được

## Hướng mở rộng tiếp theo

Các kiểm tra hiện tại mới là baseline tốt cho local MVP.

Các hướng mở rộng hợp lý tiếp theo:

- freshness checks
- row-count drift checks theo từng ngày
- completeness checks theo partition
- accepted values checks cho enum như `order_status`
- cross-table reconciliation checks

Ví dụ:

- tổng `payment_value` ở `daily_sales_gold` gần khớp aggregate từ `payments_silver`
- `customer_ltv_gold.order_count` khớp với aggregate từ `orders_silver`
