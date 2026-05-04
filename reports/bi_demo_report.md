# BI Demo Report

## Overview

Report này dùng các bảng:

- `hive.analytics.daily_sales_gold`
- `hive.analytics.category_sales_gold`
- `hive.analytics.customer_ltv_gold`

Nguồn dữ liệu được query qua `Trino`.

## Recommended visuals

### 1. Daily sales trend

Nguồn:

- `reports/bi_demo_outputs/daily_sales_trend.csv`

Chart gợi ý:

- line chart theo `order_date`
- series chính: `payment_value`
- series phụ: `order_count`

### 2. Monthly sales summary

Nguồn:

- `reports/bi_demo_outputs/monthly_sales_summary.csv`

Chart gợi ý:

- column chart theo `sales_month`
- metric: `total_payment_value`

### 3. Top categories by revenue

Nguồn:

- `reports/bi_demo_outputs/top_categories_by_revenue.csv`

Chart gợi ý:

- horizontal bar chart
- x-axis: `gross_item_value`
- y-axis: `category_name`

### 4. Top customers by LTV

Nguồn:

- `reports/bi_demo_outputs/top_customers_by_ltv.csv`

Chart gợi ý:

- table hoặc horizontal bar chart
- metric chính: `lifetime_value`
- field phụ: `order_count`, `avg_order_value`, `customer_state`

## Suggested talking points

### Sales trend

- xác định giai đoạn tăng trưởng mạnh nhất theo ngày hoặc theo tháng
- đối chiếu `payment_value` với `order_count` để xem tăng trưởng đến từ nhiều đơn hơn hay giá trị đơn cao hơn

### Category performance

- xác định category doanh thu cao nhất
- so sánh category có doanh thu cao với category có số đơn cao
- xem freight có chiếm tỷ trọng lớn ở category nào

### Customer value

- xác định nhóm khách hàng có `lifetime_value` cao nhất
- quan sát phân bố top khách theo `state`
- xem `avg_order_value` có tập trung ở một vài khách hàng hay không

## Demo checklist

- `[ ]` chạy `bash scripts/export_bi_demo_assets.sh`
- `[ ]` kiểm tra đủ 4 file CSV trong `reports/bi_demo_outputs/`
- `[ ]` dựng 3-4 chart trong Metabase hoặc spreadsheet
- `[ ]` cập nhật report bằng ảnh chart hoặc số liệu nổi bật
