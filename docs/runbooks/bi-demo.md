# BI Demo Guide

## Mục tiêu

Phần BI trong project này được chia thành 3 lớp:

1. bộ `SQL` demo để đọc các bảng `gold`
2. `Metabase` để dựng dashboard
3. report demo để export dữ liệu từ `Trino` và trình bày nhanh

Project hiện đã đủ điều kiện để làm BI vì đã có:

- `gold` tables trên `Hudi`
- query path qua `Trino`
- orchestration bằng `Airflow`
- quality checks và reconciliation checks

## Phần 1: BI demo queries

File:

- [sql/queries/bi_demo_queries.sql](/home/dohaidang/bigdata_hudi/sql/queries/bi_demo_queries.sql:1)

### Dùng ở đâu

Bạn có thể dùng file này ở 3 chỗ:

- `Trino CLI`
- `Metabase SQL editor`
- report export scripts

### Cách chạy nhanh

```bash
bash scripts/run_bi_demo_queries.sh
```

Script:

- [scripts/run_bi_demo_queries.sh](/home/dohaidang/bigdata_hudi/scripts/run_bi_demo_queries.sh:1)

### Nội dung chính của bộ query

- daily sales trend
- monthly sales summary
- top categories by revenue
- top categories by order count
- top customers by lifetime value
- top states by lifetime value
- monthly category leaders
- payment vs freight efficiency

## Phần 2: Metabase dashboard

### Files liên quan

- [docker-compose.yml](/home/dohaidang/bigdata_hudi/docker-compose.yml:1)
- [docker/trino/config.properties](/home/dohaidang/bigdata_hudi/docker/trino/config.properties:1)

### Cách khởi động

```bash
docker compose up -d metabase-postgres metabase
docker compose ps metabase-postgres metabase
```

### Truy cập ở đâu

Mở:

- `http://localhost:3000`

### Lần đầu sử dụng

Metabase sẽ hỏi bạn:

1. tạo admin account
2. kết nối database

### Cấu hình database để đọc Trino

Chọn loại database:

- `Presto`

Điền:

- Display name: `Trino Lakehouse`
- Host: `trino`
- Port: `8080`
- Catalog: `hive`
- Schema: `analytics`
- Username: `trino`
- Password: để trống
- SSL: tắt trong local stack này

Lý do dùng `Presto`:

- Metabase có driver chính thức cho Presto
- Trino trong stack này đã bật tương thích legacy header của client `Presto`

Lưu ý quan trọng:

- Metabase hiện cung cấp driver `Presto`, không phải driver `Trino`
- Trino mới dùng header `X-Trino-*`, còn client Presto cũ dùng `X-Presto-*`
- vì vậy stack này đã bật:

```text
protocol.v1.alternate-header-name=Presto
```

để Trino chấp nhận client kiểu Presto từ Metabase

Sau khi connect xong, bạn sẽ thấy 3 bảng chính:

- `daily_sales_gold`
- `category_sales_gold`
- `customer_ltv_gold`

### Nếu vừa sửa cấu hình mà vẫn lỗi auth

Bạn cần restart lại `trino` để nạp config mới:

```bash
docker compose up -d --force-recreate trino
docker compose logs --tail=80 trino
```

Sau đó tạo lại database connection trong Metabase.

### Nên dựng chart nào trước

- line chart: daily payment value theo ngày
- bar chart: top categories by revenue
- bar chart: top customers by lifetime value
- table: monthly sales summary

## Phần 3: Report demo đọc từ Trino

### Files liên quan

- [scripts/export_bi_demo_assets.sh](/home/dohaidang/bigdata_hudi/scripts/export_bi_demo_assets.sh:1)
- [reports/bi_demo_report.md](/home/dohaidang/bigdata_hudi/reports/bi_demo_report.md:1)

### Dùng ở đâu

Phần này dùng khi bạn muốn:

- trình bày project nhanh mà chưa cần dashboard tương tác
- export dữ liệu mẫu sang CSV
- đưa số liệu sang Google Sheets, Excel, PowerPoint hoặc Markdown report

### Cách chạy

```bash
bash scripts/export_bi_demo_assets.sh
```

CSV output sẽ được tạo tại:

- `reports/bi_demo_outputs/`

Các file export hiện có:

- `daily_sales_trend.csv`
- `monthly_sales_summary.csv`
- `top_categories_by_revenue.csv`
- `top_customers_by_ltv.csv`

### Cách dùng report

Mở file:

- [reports/bi_demo_report.md](/home/dohaidang/bigdata_hudi/reports/bi_demo_report.md:1)

Rồi:

1. lấy số liệu từ `reports/bi_demo_outputs/`
2. dán chart hoặc ảnh chụp chart từ Metabase
3. dùng report đó làm tài liệu demo hoặc nộp project

## Suggested workflow

Nếu bạn muốn demo nhanh:

1. chạy `bash scripts/run_bi_demo_queries.sh`
2. export CSV bằng `bash scripts/export_bi_demo_assets.sh`
3. nếu cần dashboard thì bật `Metabase`

Nếu bạn muốn demo đẹp hơn:

1. bật `Metabase`
2. connect vào `Trino`
3. dùng cùng bộ query trong `sql/queries/bi_demo_queries.sql`
4. chụp hoặc share dashboard
