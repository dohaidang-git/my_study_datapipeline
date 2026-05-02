# Apache Hudi Trong Project Này

## 1. Hudi là gì

Apache Hudi là một `table format` và `storage management layer` dành cho data lake.

Nếu chỉ lưu dữ liệu bằng `Parquet` thông thường, project sẽ có các hạn chế:
- khó `upsert` khi một record thay đổi
- khó xử lý `delete`
- khó theo dõi phiên bản dữ liệu theo từng lần ghi
- khó lấy dữ liệu incremental cho downstream jobs

Hudi giải quyết các vấn đề đó bằng cách biến thư mục dữ liệu trên object storage thành một bảng có trạng thái, có metadata, có timeline commit và có cơ chế ghi/đọc nhất quán hơn.

Nói ngắn gọn:
- `Parquet` là file format
- `Hudi` là table layer chạy trên các file format như Parquet

## 2. Vì sao project này cần Hudi

Project hiện tại là một e-commerce pipeline với các tầng:
- `raw`
- `bronze`
- `silver`
- `gold`

Nguồn dữ liệu e-commerce không phải dữ liệu chỉ thêm mới mãi mãi.
Trong thực tế sẽ có:
- đơn hàng thay đổi trạng thái
- thông tin khách hàng được cập nhật
- sản phẩm được bổ sung hoặc chỉnh sửa
- dòng dữ liệu đến trễ
- cần chạy lại pipeline mà không muốn tạo duplicate

Đây là đúng loại bài toán mà Hudi phù hợp.

Trong project này, Hudi đóng vai trò:
- lưu bảng `bronze` theo dạng có thể `upsert`
- lưu bảng `silver` theo dạng đã làm sạch nhưng vẫn giữ khả năng cập nhật
- lưu bảng `gold` theo dạng serving table có thể query qua `Trino`
- tạo nền cho các tính năng nâng cao sau này như:
  - incremental pull
  - time travel
  - CDC-style processing
  - compaction và clustering

## 3. Vai trò cụ thể của Hudi trong từng tầng

### `bronze`

Mục tiêu của `bronze` là ingest dữ liệu gần với source nhất.

Khi dùng Hudi ở tầng này:
- mỗi bảng có `record key`
- có `precombine field` để chọn phiên bản record mới hơn
- nếu ingest lại cùng business key thì Hudi có thể cập nhật thay vì chỉ append mù

Ví dụ trong project:
- `orders_bronze`: `record_key = order_id`
- `order_items_bronze`: `record_key = order_item_key`
- `payments_bronze`: `record_key = payment_key`

### `silver`

Mục tiêu của `silver` là dữ liệu sạch, chuẩn hóa, deduplicate, ready for analytics.

Khi dùng Hudi ở tầng này:
- mỗi bảng `silver` vẫn giữ được logic cập nhật
- các bản chạy lại không bắt buộc phải rewrite toàn bộ dataset
- downstream `gold` có thể đọc snapshot nhất quán hơn

Ví dụ:
- `orders_silver` có thể update khi `order_status` thay đổi
- `products_silver` có thể merge enrichment mới vào cùng `product_id`

### `gold`

Mục tiêu của `gold` là tạo bảng phục vụ business query.

Trong project này, `gold` gồm:
- `daily_sales_gold`
- `category_sales_gold`
- `customer_ltv_gold`

Dùng Hudi ở tầng `gold` không phải lúc nào cũng bắt buộc, nhưng có ích khi:
- bảng aggregate cần refresh nhiều lần
- cần publish lại dữ liệu mà không muốn drop/rebuild thủ công
- muốn query bằng `Trino` như một bảng managed rõ ràng trên lakehouse

## 4. Hudi đang làm gì trong code hiện tại

Trong code hiện tại, Hudi nằm chủ yếu ở các helper chung:
- [pipelines/common/hudi_writer.py](/home/dohaidang/bigdata_hudi/pipelines/common/hudi_writer.py:1)
- [pipelines/common/bronze_job.py](/home/dohaidang/bigdata_hudi/pipelines/common/bronze_job.py:1)
- [pipelines/common/silver_job.py](/home/dohaidang/bigdata_hudi/pipelines/common/silver_job.py:1)
- [pipelines/common/gold_job.py](/home/dohaidang/bigdata_hudi/pipelines/common/gold_job.py:1)
- [pipelines/common/paths.py](/home/dohaidang/bigdata_hudi/pipelines/common/paths.py:1)

Luồng hiện tại được thiết kế theo hướng:
- `bronze` mặc định ghi vào `s3a://lakehouse/bronze/...`
- `silver` mặc định đọc từ `bronze Hudi` và ghi vào `s3a://lakehouse/silver/...`
- `gold` mặc định đọc từ `silver Hudi` và ghi vào `s3a://lakehouse/gold/...`

Điều này có nghĩa là Hudi không còn là một nhánh tùy chọn mang tính minh họa, mà đang trở thành đường mặc định của pipeline.

## 5. Khái niệm quan trọng của Hudi bạn cần hiểu trong project này

### `record key`

Là khóa business dùng để nhận diện một record duy nhất.

Ví dụ:
- `order_id`
- `payment_key`
- `customer_id`

Nếu chọn sai `record key`, Hudi sẽ không `upsert` đúng.

### `precombine field`

Là cột dùng để quyết định record nào là phiên bản mới hơn khi cùng `record key`.

Ví dụ:
- `order_purchase_timestamp`
- `_ingested_at`
- `last_order_date`

Nếu chọn `precombine field` không phản ánh được tính mới/cũ, update có thể bị ghi đè sai.

### `partition path`

Là cách chia dữ liệu thành thư mục vật lý để query hiệu quả hơn.

Ví dụ trong project:
- `orders_bronze` partition theo `dt`
- `orders_silver` partition theo `order_purchase_date`
- `daily_sales_gold` partition theo `order_date`

### `COPY_ON_WRITE`

Đây là table type hiện đang dùng trong writer.

Ý nghĩa:
- đọc đơn giản hơn
- query bằng engine như `Trino` dễ hơn
- phù hợp với MVP và batch analytics

Đổi lại:
- update nhiều sẽ rewrite file nhiều hơn `MERGE_ON_READ`

Với project hiện tại, chọn `COPY_ON_WRITE` là hợp lý hơn `MERGE_ON_READ`.

## 6. Hudi khác gì so với Parquet thường

### Chỉ dùng Parquet

Ưu điểm:
- đơn giản
- dễ hiểu
- dễ debug lúc đầu

Nhược điểm:
- không có khái niệm bảng có state
- update/delete phải tự xử lý bằng job logic
- rerun dễ gây duplicate hoặc phải overwrite cả thư mục
- không có commit timeline đúng nghĩa

### Dùng Hudi

Ưu điểm:
- có `upsert`
- có `delete`
- có timeline commit
- có incremental consumption
- phù hợp cho data lakehouse hơn

Nhược điểm:
- setup phức tạp hơn
- cần thêm jars và config Spark/Hive/Trino
- cần hiểu `record key`, `precombine`, `partition`

## 7. So sánh Hudi với các nền tảng tương tự

### Hudi vs Delta Lake

`Delta Lake` mạnh khi:
- hệ sinh thái nghiêng về Databricks/Spark
- cần transaction và DML rất thuận tiện
- team làm việc chủ yếu quanh Spark SQL

`Hudi` mạnh khi:
- bài toán thiên về ingestion, CDC, incremental processing
- muốn tối ưu cho update-heavy pipeline trên data lake
- cần kết hợp object storage + metastore + engine query mở

Trong project này:
- nếu mục tiêu là demo pipeline e-commerce có `upsert` và incremental behavior, Hudi hợp hơn
- nếu mục tiêu là notebook analytics rất gắn với Spark/Databricks, Delta có thể tiện hơn

### Hudi vs Apache Iceberg

`Iceberg` mạnh khi:
- cần table format rất chuẩn cho analytics engine đa dạng
- partition evolution, schema evolution, metadata management là ưu tiên lớn
- hệ truy vấn có nhiều engine đọc như Trino, Spark, Flink, Athena

`Hudi` mạnh khi:
- ingestion/update là trọng tâm
- cần thao tác gần với CDC
- cần tối ưu write path và record-level update

Trong project này:
- `Iceberg` cũng là lựa chọn tốt nếu mục tiêu chính là analytics table management
- `Hudi` hợp hơn khi muốn kể câu chuyện `data pipeline có dữ liệu thay đổi và cần upsert`

### Hudi vs Hive external Parquet table

`Hive external table` trên Parquet chỉ là metadata trỏ vào file.
Nó không tự mang lại:
- record-level update
- timeline
- incremental query

Hudi thì có những thứ đó.

Vì vậy trong project này:
- `Hive Metastore` chỉ là lớp metadata catalog
- `Hudi` mới là lớp quản lý bảng dữ liệu trên storage

## 8. Vì sao Hudi là lựa chọn hợp lý cho bài toán e-commerce này

E-commerce là domain có nhiều thay đổi record:
- order thay đổi trạng thái
- customer profile thay đổi
- sản phẩm được cập nhật
- batch chạy lại có thể đến cùng business key

Nếu chỉ demo batch transform một lần, Hudi không nổi bật.
Nhưng nếu project muốn thể hiện:
- `upsert`
- deduplicate
- rerun an toàn
- lakehouse table management

thì Hudi là lựa chọn hợp lý.

Đó cũng là lý do trong project này, Hudi không chỉ là “công nghệ cho có”, mà là thành phần trung tâm của kiến trúc.

## 9. Hudi chưa giải quyết giúp bạn điều gì

Hudi không tự thay thế toàn bộ hệ thống.
Nó không tự lo:
- orchestration
- data quality policy
- dashboard
- BI modeling
- business metric definition

Trong project này, Hudi cần phối hợp với:
- `Spark` để transform và write
- `MinIO` để làm object storage
- `Hive Metastore` để catalog metadata
- `Trino` để query
- `Airflow` cho orchestration về sau

## 10. Kết luận cho project hiện tại

Vai trò của Hudi trong project này là:
- biến các thư mục dữ liệu trên object storage thành các bảng có khả năng update
- làm nền cho pipeline `bronze -> silver -> gold`
- giúp project thể hiện rõ tính chất lakehouse thay vì chỉ là ETL batch ghi Parquet

Nếu phải tóm lại trong một câu:

`Hudi là thành phần giúp project này chuyển từ file-based ETL sang table-based lakehouse pipeline có khả năng upsert và quản lý trạng thái dữ liệu tốt hơn.`
