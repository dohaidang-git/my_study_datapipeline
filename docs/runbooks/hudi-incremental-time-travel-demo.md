# Hudi Incremental Upsert And Time Travel Demo

## Mục tiêu

Phần demo này được tạo để chứng minh 2 điểm mạnh cốt lõi của `Hudi`:

1. `incremental upsert`
2. `time travel`

Demo dùng một bảng riêng:

- `s3a://lakehouse/demo/payments_bronze_demo`

Lý do:

- không làm bẩn các bảng chính của pipeline
- vẫn dùng đúng runtime Hudi của project
- dễ chụp màn hình và mô tả trong báo cáo

## Các file chính

- [pipelines/tools/prepare_hudi_payments_demo.py](/home/dohaidang/bigdata_hudi/pipelines/tools/prepare_hudi_payments_demo.py:1)
- [pipelines/tools/run_hudi_incremental_upsert_demo.py](/home/dohaidang/bigdata_hudi/pipelines/tools/run_hudi_incremental_upsert_demo.py:1)
- [pipelines/tools/run_hudi_time_travel_demo.py](/home/dohaidang/bigdata_hudi/pipelines/tools/run_hudi_time_travel_demo.py:1)
- [scripts/run_hudi_incremental_demo.sh](/home/dohaidang/bigdata_hudi/scripts/run_hudi_incremental_demo.sh:1)

## Demo flow

### Bước 1: Chuẩn bị bảng demo

Script sẽ lấy một tập nhỏ từ `payments_bronze` và ghi thành bảng Hudi demo mới.

Chạy:

```bash
bash scripts/spark_submit_container.sh pipelines/tools/prepare_hudi_payments_demo.py
```

Kết quả:

- tạo snapshot ban đầu
- in commit instant đầu tiên
- in các dòng demo ban đầu

### Bước 2: Chạy incremental upsert

Script upsert sẽ:

- cập nhật một `payment_key` đã có
- chèn thêm một `payment_key` mới

Chạy:

```bash
bash scripts/spark_submit_container.sh pipelines/tools/run_hudi_incremental_upsert_demo.py
```

Kết quả:

- in `before instant`
- in `after instant`
- cho thấy một record bị update
- cho thấy một record mới được insert

Report tóm tắt sẽ được ghi vào:

- [reports/hudi_demo/incremental_upsert_summary.md](/home/dohaidang/bigdata_hudi/reports/hudi_demo/incremental_upsert_summary.md:1)

### Bước 3: Chạy time travel

Đọc snapshot cũ:

```bash
bash scripts/spark_submit_container.sh pipelines/tools/run_hudi_time_travel_demo.py --use-previous
```

Đọc snapshot mới nhất:

```bash
bash scripts/spark_submit_container.sh pipelines/tools/run_hudi_time_travel_demo.py
```

Ý nghĩa:

- snapshot cũ cho thấy trạng thái trước upsert
- snapshot mới cho thấy trạng thái sau upsert
- cùng một bảng Hudi, nhưng đọc ở 2 thời điểm commit khác nhau

Mỗi lần chạy time travel cũng sẽ sinh report:

- [reports/hudi_demo/time_travel_summary_20260503102450556.md](/home/dohaidang/bigdata_hudi/reports/hudi_demo/time_travel_summary_20260503102450556.md:1)
- [reports/hudi_demo/time_travel_summary_20260503103015151.md](/home/dohaidang/bigdata_hudi/reports/hudi_demo/time_travel_summary_20260503103015151.md:1)

Trong đó:

- report ở instant cũ sẽ cho thấy `payment_key` được update vẫn còn giá trị cũ
- report ở instant mới sẽ cho thấy record đó đã đổi giá trị và record insert mới đã xuất hiện

## Chạy toàn bộ demo trong một lệnh

```bash
bash scripts/run_hudi_incremental_demo.sh
```

## Những gì nên đưa vào báo cáo

### Phần incremental upsert

- commit instant trước khi upsert
- commit instant sau khi upsert
- record được update
- record được insert
- giải thích vì sao đây là điểm mạnh hơn so với overwrite cả bảng

### Phần time travel

- snapshot cũ
- snapshot mới
- so sánh trực tiếp giá trị của `payment_key` được update

## Gợi ý câu mô tả cho báo cáo

- Hudi cho phép cập nhật record-level dựa trên `record key` và `precombine field` thay vì ghi đè toàn bộ bảng.
- Sau mỗi lần ghi, Hudi lưu một timeline commit, từ đó có thể đọc lại snapshot của bảng tại một thời điểm trước đó bằng cơ chế time travel.
- Trong project này, demo `payments_bronze_demo` cho thấy một record được update và một record được insert thông qua `upsert`, sau đó có thể đọc lại snapshot trước update bằng `as.of.instant`.
