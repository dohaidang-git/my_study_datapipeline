# Các File Python Trong Project

## 1. Mục tiêu của tài liệu này

Tài liệu này giúp bạn đọc phần Python của project theo 3 câu hỏi:

- file này dùng để làm gì
- các hàm trong file làm gì và chạy như thế nào
- dữ liệu đi qua các file đó ra sao

Project hiện tại là một pipeline lakehouse theo luồng:

`raw CSV -> bronze -> silver -> gold -> Hudi on MinIO -> Hive Metastore -> Trino`

Phần Python chủ yếu nằm trong thư mục:

- `pipelines/common/`
- `pipelines/bronze/`
- `pipelines/silver/`
- `pipelines/gold/`
- `pipelines/tools/`

## 2. Luồng dữ liệu tổng thể

### 2.1. Từ raw sang bronze

Nguồn vào là các file CSV trong `data/raw/olist/`.

Ví dụ:
- `olist_orders_dataset.csv`
- `olist_order_items_dataset.csv`
- `olist_customers_dataset.csv`
- `olist_order_reviews_dataset.csv`

Mỗi job bronze sẽ:

1. parse argument đầu vào/đầu ra
2. tạo `SparkSession`
3. đọc CSV bằng `read_csv_source()`
4. chạy `transform()` để cast kiểu dữ liệu cơ bản
5. thêm metadata bằng `finalize_bronze_df()`
6. ghi ra `Hudi` hoặc `parquet`

Đầu ra mặc định bây giờ là:
- `s3a://lakehouse/bronze/<table_name>`

### 2.2. Từ bronze sang silver

Mỗi job silver sẽ:

1. đọc bảng bronze bằng `read_table_source()`
2. làm sạch dữ liệu bằng `transform()`
3. deduplicate, chuẩn hóa text, cast lại cột, enrich nhẹ
4. ghi ra `s3a://lakehouse/silver/<table_name>`

Silver là tầng dữ liệu sạch hơn, đã có business meaning rõ hơn.

### 2.3. Từ silver sang gold

Các job gold đọc nhiều bảng silver để tạo bảng business mart.

Hiện tại có 3 bảng chính:

- `daily_sales_gold`
- `category_sales_gold`
- `customer_ltv_gold`

Các job này chủ yếu làm:
- join
- aggregate
- tính metric business
- ghi ra `s3a://lakehouse/gold/<table_name>`

### 2.4. Verify và query

Sau khi ghi xong Hudi:

- Spark có thể đọc trực tiếp bằng `spark.read.format("hudi").load(...)`
- Hive Metastore lưu metadata bảng
- Trino query các bảng này qua catalog `hive`

## 3. Nhóm `pipelines/common`

Đây là nhóm file quan trọng nhất để hiểu project, vì toàn bộ job bronze/silver/gold đều dựa vào các helper ở đây.

### 3.1. `pipelines/common/runtime.py`

File này có 1 hàm:

- `ensure_project_root_on_path()`

Vai trò:
- thêm root project vào `sys.path`
- giúp các script Python có thể import `pipelines.*` khi chạy trực tiếp

Ý nghĩa thực tế:
- nếu chạy file kiểu `python pipelines/...py` hoặc `spark-submit pipelines/...py` thì Python thường không tự hiểu root package
- hàm này sửa chuyện đó

### 3.2. `pipelines/common/paths.py`

File này định nghĩa các path chuẩn của project.

Hàm chính:
- `raw_olist_path(filename)`
- `bronze_local_path(table_name)`
- `bronze_hudi_path(table_name)`
- `silver_local_path(table_name)`
- `silver_hudi_path(table_name)`
- `gold_local_path(table_name)`
- `gold_hudi_path(table_name)`

Vai trò:
- tránh hard-code path ở nhiều file
- gom toàn bộ logic đường dẫn vào một nơi

Điều cần hiểu:
- local path kiểu `data/bronze/...` dùng khi test `parquet`
- Hudi path kiểu `s3a://lakehouse/...` dùng cho flow chính hiện tại

### 3.3. `pipelines/common/metadata.py`

Hàm chính:
- `add_ingestion_metadata(df, batch_id, source_file, source_system)`

Vai trò:
- thêm metadata ingestion vào DataFrame

Các cột được thêm:
- `_ingested_at`
- `_batch_id`
- `_source_file`
- `_source_system`

Ý nghĩa:
- biết record đến từ đâu
- biết được batch nào sinh ra record
- hỗ trợ trace lineage và debug

### 3.4. `pipelines/common/job_args.py`

File này xử lý argument cho bronze jobs.

Hàm chính:
- `build_bronze_job_parser(...)`
- `parse_bronze_job_args(...)`

Vai trò:
- chuẩn hóa CLI arguments cho job bronze

Các argument quan trọng:
- `--input-path`
- `--output-path`
- `--output-format`
- `--batch-id`
- `--source-system`
- `--mode`

Điều cần hiểu:
- `output-format` cho phép chạy `parquet` hoặc `hudi`
- hiện mặc định là `hudi`
- `mode` chỉ thực sự có ý nghĩa rõ với `parquet`; với Hudi thì logic là `upsert`

### 3.5. `pipelines/common/spark_session.py`

Đây là file bootstrap Spark runtime.

Hàm chính:
- `discover_local_jars()`
- `build_spark_session(app_name, extra_conf=None, enable_hive_support=False)`

#### `discover_local_jars()`

Vai trò:
- quét thư mục `jars/`
- ưu tiên bộ jars đã được xác nhận tương thích

Hiện tại bộ jars ưu tiên là:
- `hudi-spark3.5-bundle_2.12-1.1.1.jar`
- `hadoop-aws-3.3.4.jar`
- `aws-java-sdk-bundle-1.12.262.jar`

Ý nghĩa:
- tránh dùng nhầm jar không khớp với Spark runtime trong container

#### `build_spark_session(...)`

Vai trò:
- tạo `SparkSession`
- set timezone
- set `partitionOverwriteMode`
- nếu phát hiện Hudi jar thì bật `KryoSerializer`

Điểm quan trọng:
- file này hiện **không tự bật** `HoodieCatalog`
- file này hiện **không mặc định bật** `enableHiveSupport()`

Lý do:
- trong project này ta đang chạy Hudi theo kiểu `path-based`
- cách đó ổn định hơn với local stack đang dùng

### 3.6. `pipelines/common/hudi_writer.py`

File này bọc logic ghi Hudi.

Hàm chính:
- `build_hudi_options(...)`
- `write_hudi_table(...)`

#### `build_hudi_options(...)`

Vai trò:
- tạo ra dictionary options cho `df.write.format("hudi")`

Các option quan trọng:
- `hoodie.table.name`
- `hoodie.datasource.write.table.type = COPY_ON_WRITE`
- `hoodie.datasource.write.operation = upsert`
- `hoodie.datasource.write.recordkey.field`
- `hoodie.datasource.write.precombine.field`

Điều cần hiểu:
- `record_key` quyết định record nào là duy nhất
- `precombine_field` quyết định bản nào là bản mới hơn khi trùng key
- nếu chọn sai 2 cột này thì Hudi có thể làm mất dòng hoặc update sai

#### `write_hudi_table(...)`

Vai trò:
- nhận DataFrame
- build options
- gọi `df.write.format("hudi").save(output_path)`

### 3.7. `pipelines/common/bronze_job.py`

Đây là helper chung cho bronze.

Hàm chính:
- `read_csv_source(...)`
- `finalize_bronze_df(...)`
- `write_bronze_output(...)`
- `add_date_partition_column(...)`

#### `read_csv_source(...)`

Vai trò:
- đọc CSV với cấu hình mặc định chung
- cho phép override bằng `options`

Điểm quan trọng:
- file reviews dùng `multiLine=true`
- đây là lý do job reviews không còn bị lệch cột

#### `finalize_bronze_df(...)`

Vai trò:
- gọi `add_ingestion_metadata()`
- chuẩn hóa bước cuối trước khi ghi bronze

#### `write_bronze_output(...)`

Vai trò:
- nếu `output_format=parquet` thì ghi parquet
- nếu `output_format=hudi` thì gọi `write_hudi_table()`

#### `add_date_partition_column(...)`

Vai trò:
- thêm cột partition date, thường tên là `dt`

### 3.8. `pipelines/common/silver_job.py`

Đây là helper chung cho silver.

Hàm chính:
- `parse_silver_job_args(...)`
- `read_parquet_source(...)`
- `read_hudi_source(...)`
- `read_table_source(...)`
- `write_silver_output(...)`

Điều cần hiểu:
- silver có thể đọc từ `parquet` hoặc `hudi`
- nhưng mặc định bây giờ là `hudi`

### 3.9. `pipelines/common/gold_job.py`

Cấu trúc tương tự silver, nhưng dành cho gold.

Hàm chính:
- `parse_gold_job_args(...)`
- `read_parquet_source(...)`
- `read_hudi_source(...)`
- `read_table_source(...)`
- `write_gold_output(...)`

Ý nghĩa:
- gold jobs không tự viết logic ghi lặp lại
- mọi logic đọc/ghi được gom vào helper chung

## 4. Nhóm `pipelines/bronze`

Mỗi file bronze đều có cấu trúc gần giống nhau:

1. định nghĩa `TABLE_NAME`, `SOURCE_FILE`
2. viết `transform(df)`
3. trong `main()`:
   - parse args
   - tạo SparkSession
   - đọc raw CSV
   - transform
   - thêm metadata
   - ghi output

### 4.1. Các bronze jobs chính

- `load_orders_bronze.py`
- `load_order_items_bronze.py`
- `load_customers_bronze.py`
- `load_payments_bronze.py`
- `load_products_bronze.py`
- `load_sellers_bronze.py`
- `load_reviews_bronze.py`
- `load_geolocation_bronze.py`
- `load_product_category_translation_bronze.py`

### 4.2. Ví dụ `load_orders_bronze.py`

`transform(df)` làm:
- cast các cột timestamp
- chuẩn hóa `order_status`
- thêm cột `dt`

`main()` làm:
- đọc `olist_orders_dataset.csv`
- ghi vào `orders_bronze`

### 4.3. Trường hợp đặc biệt `load_reviews_bronze.py`

Điểm đặc biệt:
- file CSV có comment nhiều dòng
- nên dùng `CSV_OPTIONS = {"multiLine": "true"}`

`transform(df)` làm:
- cast `review_score`
- cast timestamp review
- tạo `bronze_record_key`

Tại sao cần `bronze_record_key`:
- nếu dùng `review_id` trực tiếp làm Hudi record key, bronze có thể bị mất dòng do upsert trùng key
- bronze trong project này cần giữ gần nguyên raw, nên key phải đủ duy nhất ở mức record ingest

### 4.4. Trường hợp đặc biệt `load_geolocation_bronze.py`

File này có:
- `geolocation_key`: business key logic
- `bronze_record_key`: key duy nhất để ghi Hudi bronze

Ý nghĩa:
- `geolocation_key` dùng cho silver deduplicate
- `bronze_record_key` dùng để đảm bảo bronze không collapse dữ liệu gốc

Đây là một điểm rất quan trọng để hiểu triết lý project:

- `bronze` giữ gần raw
- `silver` mới là nơi xử lý deduplicate theo business key

## 5. Nhóm `pipelines/silver`

Silver là tầng clean data.

Mỗi file silver thường có:
- `TABLE_NAME`
- `INPUT_TABLE`
- `OUTPUT_PATH`
- `transform(df)` hoặc `transform(df1, df2)`
- `main()`

### 5.1. `load_orders_silver.py`

`transform(df)` làm:
- `dropDuplicates(["order_id"])`
- chuẩn hóa `order_status`
- tạo `order_purchase_date`
- tính `delivery_delay_days`

Ý nghĩa:
- từ bảng bronze thô, tạo ra bảng orders sạch hơn, dùng tốt cho analytics

### 5.2. `load_order_items_silver.py`

Logic chính:
- deduplicate theo `order_item_key`
- giữ granularity mỗi order item

### 5.3. `load_customers_silver.py`

Logic chính:
- chuẩn hóa city/state
- loại bỏ trùng khách hàng theo `customer_id`

### 5.4. `load_payments_silver.py`

Logic chính:
- deduplicate theo `payment_key`
- cast numeric
- lọc hoặc chuẩn hóa giá trị thanh toán

### 5.5. `load_products_silver_base.py`

Logic chính:
- làm sạch bảng products gốc
- đây là base table, chưa enrich translation

### 5.6. `load_product_category_translation_silver.py`

Logic chính:
- chuẩn hóa bảng mapping category tiếng Bồ Đào Nha sang tiếng Anh

### 5.7. `load_products_silver.py`

Đây là silver job có join.

Hàm:
- `parse_args()`
- `transform(products_df, translation_df)`

`transform(...)` làm:
- join `products_silver_base`
- với `product_category_translation_silver`
- bổ sung `product_category_name_english`

Ý nghĩa:
- đây là ví dụ điển hình cho derived silver table

### 5.8. `load_reviews_silver.py`

Logic chính:
- deduplicate theo `review_id`
- reviews bắt đầu được gộp theo business key ở tầng này

### 5.9. `load_geolocation_silver.py`

Logic chính:
- `dropDuplicates(["geolocation_key"])`
- chuẩn hóa city/state

Điểm cần hiểu:
- geolocation bị giảm row count từ bronze sang silver là đúng, vì silver đang hợp nhất record trùng theo key logic

## 6. Nhóm `pipelines/gold`

Gold là tầng business mart.

Mỗi file gold thường:
- đọc nhiều bảng silver
- join hoặc aggregate
- tính metric business
- ghi ra bảng gold

### 6.1. `build_daily_sales_gold.py`

Hàm:
- `build_order_items_agg(order_items_df)`
- `build_payments_agg(payments_df)`
- `transform(orders_df, order_items_df, payments_df)`

Ý tưởng chính:
- aggregate `order_items` theo `order_id`
- aggregate `payments` theo `order_id`
- join vào `orders`
- sau đó group theo `order_date`

Điều quan trọng:
- không join trực tiếp `order_items` và `payments` ở mức chi tiết
- nếu làm vậy dễ bị nhân dòng và làm sai doanh thu

### 6.2. `build_category_sales_gold.py`

Hàm:
- `transform(order_items_df, products_df, orders_df)`

Logic:
- join order item với product
- join thêm order date/status
- suy ra `category_name`
- aggregate theo `order_purchase_date` và `category_name`

### 6.3. `build_customer_ltv_gold.py`

Hàm:
- `build_payments_agg(payments_df)`
- `transform(orders_df, customers_df, payments_df)`

Logic:
- aggregate payment theo `order_id`
- join orders với customers
- group theo customer
- tính:
  - `order_count`
  - `lifetime_value`
  - `first_order_date`
  - `last_order_date`
  - `customer_lifespan_days`
  - `avg_order_value`
  - `ltv_rank`

Đây là bảng phục vụ câu hỏi business kiểu:
- khách hàng nào giá trị cao nhất
- khách hàng mua nhiều lần bao lâu

## 7. Nhóm `pipelines/tools`

Đây là các file phục vụ verify, không phải ETL chính.

### 7.1. `verify_hudi_orders_read.py`

Vai trò:
- smoke test đọc Hudi table `orders_bronze`

### 7.2. `verify_hudi_pipeline.py`

Vai trò:
- đọc toàn bộ các Hudi tables bronze/silver/gold
- đếm số dòng
- so sánh với expected counts

Ý nghĩa:
- đây là file verify end-to-end hiện tại của project

Nếu verify pass, nghĩa là:
- write path Hudi đang ổn
- read path Hudi đang ổn
- row count sau migration từ parquet sang Hudi không bị lệch

## 8. Dữ liệu được vận chuyển ra sao

Đây là phần quan trọng nhất để hiểu project.

### 8.1. Tầng bronze

Ví dụ với `orders`:

1. `raw_olist_path("olist_orders_dataset.csv")`
2. `read_csv_source()`
3. `transform()`
4. `finalize_bronze_df()`
5. `write_bronze_output()`
6. `write_hudi_table()`
7. ghi vào `s3a://lakehouse/bronze/orders_bronze`

### 8.2. Tầng silver

Ví dụ với `orders_silver`:

1. `read_table_source()` đọc `s3a://lakehouse/bronze/orders_bronze`
2. `transform()`:
   - dedup
   - chuẩn hóa
   - thêm cột phân tích
3. `write_silver_output()`
4. ghi vào `s3a://lakehouse/silver/orders_silver`

### 8.3. Tầng gold

Ví dụ với `daily_sales_gold`:

1. đọc `orders_silver`
2. đọc `order_items_silver`
3. đọc `payments_silver`
4. aggregate về mức `order_id`
5. aggregate tiếp về mức `order_date`
6. ghi vào `s3a://lakehouse/gold/daily_sales_gold`

### 8.4. Từ Hudi sang query

1. Spark ghi Hudi vào MinIO qua `s3a://`
2. Hive Metastore giữ metadata bảng
3. Trino đọc bảng từ catalog `hive`
4. query bằng SQL như:
   - `SELECT COUNT(*) FROM hive.analytics.daily_sales_gold`

## 9. Những điểm quan trọng cần nhớ khi đọc code

### 9.1. `main()` là entrypoint

Gần như mọi job đều có `main()`.

Nếu muốn hiểu file chạy gì:
- xem `main()` trước
- rồi xem `transform()`

### 9.2. Bronze không phải nơi deduplicate business logic mạnh

Đặc biệt sau khi chuyển sang Hudi, cần nhớ:
- bronze phải giữ gần raw
- silver mới là nơi deduplicate theo business key

Vì vậy:
- `reviews_bronze`
- `geolocation_bronze`

được thiết kế `record_key` riêng để tránh mất dòng gốc.

### 9.3. `record_key` và `precombine_field` là trái tim của Hudi

Khi đọc code Hudi, luôn tự hỏi:
- record nào được xem là duy nhất
- cột nào quyết định bản mới hơn

Nếu trả lời sai, pipeline vẫn chạy nhưng dữ liệu có thể sai.

### 9.4. Gold phải kiểm soát granularity trước khi aggregate

Không phải cứ join nhiều bảng rồi `groupBy` là đúng.

Trong project này, gold jobs đã cố ý:
- aggregate về `order_id` trước
- rồi mới aggregate tiếp lên business level

Đó là cách tránh double-count.

## 10. Thứ tự đọc code nếu bạn mới vào project

Thứ tự nên đọc:

1. `pipelines/common/paths.py`
2. `pipelines/common/spark_session.py`
3. `pipelines/common/hudi_writer.py`
4. `pipelines/common/bronze_job.py`
5. `pipelines/common/silver_job.py`
6. `pipelines/common/gold_job.py`
7. `pipelines/bronze/load_orders_bronze.py`
8. `pipelines/silver/load_orders_silver.py`
9. `pipelines/gold/build_daily_sales_gold.py`
10. `pipelines/tools/verify_hudi_pipeline.py`

Nếu đọc theo thứ tự này, bạn sẽ hiểu:
- path ở đâu ra
- Spark session được dựng thế nào
- Hudi được ghi thế nào
- một job bronze/silver/gold nối với nhau ra sao

## 11. Kết luận ngắn

Phần Python của project hiện tại có thể hiểu theo mô hình:

- `common`: framework nhỏ của project
- `bronze`: ingest và cast cơ bản
- `silver`: clean và chuẩn hóa
- `gold`: aggregate business
- `tools`: verify và smoke test

Luồng dữ liệu chính là:

`raw CSV -> Spark DataFrame -> Hudi bronze -> Hudi silver -> Hudi gold -> Hive/Trino`

Khi đọc hoặc sửa code, luôn xác định rõ:

- bạn đang ở tầng nào
- bảng đang ở granularity nào
- `record_key` của Hudi là gì
- downstream nào sẽ đọc kết quả này
