# Dữ Liệu Đầu Vào Và Cách Xử Lý Qua Các Tầng

## 1. Mục tiêu của tài liệu này

Tài liệu này trả lời 4 câu hỏi:

- dữ liệu đầu vào của project là gì
- mỗi file raw có các cột nào
- dữ liệu được xử lý ra sao khi đi qua `bronze`, `silver`, `gold`
- bảng đầu ra cuối cùng có ý nghĩa business gì

Tài liệu này tập trung vào các nguồn Olist hiện đang được dùng thực tế trong pipeline.

## 2. Tổng quan luồng dữ liệu

Luồng dữ liệu hiện tại của project là:

`CSV raw -> bronze Hudi -> silver Hudi -> gold Hudi -> Hive Metastore -> Trino`

Ý nghĩa từng tầng:

- `raw`: file gốc, gần như chưa đụng vào
- `bronze`: ingest vào Spark/Hudi, cast kiểu cơ bản, thêm metadata
- `silver`: làm sạch, chuẩn hóa, deduplicate, enrich
- `gold`: tổng hợp theo góc nhìn business

## 3. Danh sách nguồn dữ liệu đầu vào

Các file raw hiện đang được dùng:

- `data/raw/olist/olist_orders_dataset.csv`
- `data/raw/olist/olist_order_items_dataset.csv`
- `data/raw/olist/olist_customers_dataset.csv`
- `data/raw/olist/olist_order_payments_dataset.csv`
- `data/raw/olist/olist_products_dataset.csv`
- `data/raw/olist/olist_sellers_dataset.csv`
- `data/raw/olist/olist_order_reviews_dataset.csv`
- `data/raw/olist/olist_geolocation_dataset.csv`
- `data/raw/olist/product_category_name_translation.csv`

## 4. Chi tiết từng file raw và cách xử lý

## 4.1. `olist_orders_dataset.csv`

### Cột raw

- `order_id`
- `customer_id`
- `order_status`
- `order_purchase_timestamp`
- `order_approved_at`
- `order_delivered_carrier_date`
- `order_delivered_customer_date`
- `order_estimated_delivery_date`

### Ý nghĩa dữ liệu

Đây là bảng vòng đời đơn hàng. Mỗi dòng là một order.

### Xử lý ở `orders_bronze`

Job:
- [pipelines/bronze/load_orders_bronze.py](/home/dohaidang/bigdata_hudi/pipelines/bronze/load_orders_bronze.py:1)

Các xử lý chính:
- cast toàn bộ cột thời gian sang `timestamp`
- chuẩn hóa `order_status` về lowercase
- sinh thêm cột partition `dt = to_date(order_purchase_timestamp)`
- thêm metadata ingestion:
  - `_ingested_at`
  - `_batch_id`
  - `_source_file`
  - `_source_system`

### Xử lý ở `orders_silver`

Job:
- [pipelines/silver/load_orders_silver.py](/home/dohaidang/bigdata_hudi/pipelines/silver/load_orders_silver.py:1)

Các xử lý chính:
- deduplicate theo `order_id`
- chuẩn hóa lại `order_status`
- sinh `order_purchase_date`
- tính `delivery_delay_days`

### Vai trò ở `gold`

Bảng này đi vào:
- `daily_sales_gold`
- `category_sales_gold`
- `customer_ltv_gold`

## 4.2. `olist_order_items_dataset.csv`

### Cột raw

- `order_id`
- `order_item_id`
- `product_id`
- `seller_id`
- `shipping_limit_date`
- `price`
- `freight_value`

### Ý nghĩa dữ liệu

Đây là bảng chi tiết item trong đơn hàng. Một order có thể có nhiều item.

### Xử lý ở `order_items_bronze`

Job:
- [pipelines/bronze/load_order_items_bronze.py](/home/dohaidang/bigdata_hudi/pipelines/bronze/load_order_items_bronze.py:1)

Các xử lý chính:
- cast `order_item_id` sang `int`
- cast `shipping_limit_date` sang `timestamp`
- cast `price`, `freight_value` sang `decimal(12,2)`
- sinh `order_item_key = order_id + "_" + order_item_id`
- sinh cột partition `dt = to_date(shipping_limit_date)`
- thêm metadata ingestion

### Xử lý ở `order_items_silver`

Job:
- [pipelines/silver/load_order_items_silver.py](/home/dohaidang/bigdata_hudi/pipelines/silver/load_order_items_silver.py:1)

Các xử lý chính:
- deduplicate theo `order_item_key`
- cast lại `price`, `freight_value`, `shipping_limit_date`

### Vai trò ở `gold`

Bảng này đi vào:
- `daily_sales_gold`
- `category_sales_gold`

## 4.3. `olist_customers_dataset.csv`

### Cột raw

- `customer_id`
- `customer_unique_id`
- `customer_zip_code_prefix`
- `customer_city`
- `customer_state`

### Ý nghĩa dữ liệu

Đây là bảng định danh và địa lý khách hàng.

### Xử lý ở `customers_bronze`

Job:
- [pipelines/bronze/load_customers_bronze.py](/home/dohaidang/bigdata_hudi/pipelines/bronze/load_customers_bronze.py:1)

Các xử lý chính:
- cast `customer_zip_code_prefix` sang `string`
- chuẩn hóa `customer_city` bằng `trim + initcap`
- chuẩn hóa `customer_state` bằng `trim + upper`
- thêm metadata ingestion

### Xử lý ở `customers_silver`

Job:
- [pipelines/silver/load_customers_silver.py](/home/dohaidang/bigdata_hudi/pipelines/silver/load_customers_silver.py:1)

Các xử lý chính:
- deduplicate theo `customer_id`
- chuẩn hóa lại `city`, `state`, `zip`

### Vai trò ở `gold`

Bảng này đi vào:
- `customer_ltv_gold`

## 4.4. `olist_order_payments_dataset.csv`

### Cột raw

- `order_id`
- `payment_sequential`
- `payment_type`
- `payment_installments`
- `payment_value`

### Ý nghĩa dữ liệu

Đây là bảng thanh toán theo order. Một order có thể có nhiều dòng thanh toán.

### Xử lý ở `payments_bronze`

Job:
- [pipelines/bronze/load_payments_bronze.py](/home/dohaidang/bigdata_hudi/pipelines/bronze/load_payments_bronze.py:1)

Các xử lý chính:
- cast `payment_sequential` sang `int`
- chuẩn hóa `payment_type` về lowercase
- cast `payment_installments` sang `int`
- cast `payment_value` sang `decimal(12,2)`
- sinh `payment_key` từ `order_id` và `payment_sequential`
- thêm metadata ingestion

### Xử lý ở `payments_silver`

Job:
- [pipelines/silver/load_payments_silver.py](/home/dohaidang/bigdata_hudi/pipelines/silver/load_payments_silver.py:1)

Các xử lý chính:
- deduplicate theo `payment_key`
- chuẩn hóa lại `payment_type`

### Vai trò ở `gold`

Bảng này đi vào:
- `daily_sales_gold`
- `customer_ltv_gold`

## 4.5. `olist_products_dataset.csv`

### Cột raw

- `product_id`
- `product_category_name`
- `product_name_lenght`
- `product_description_lenght`
- `product_photos_qty`
- `product_weight_g`
- `product_length_cm`
- `product_height_cm`
- `product_width_cm`

### Ý nghĩa dữ liệu

Đây là bảng thuộc tính sản phẩm.

### Xử lý ở `products_bronze`

Job:
- [pipelines/bronze/load_products_bronze.py](/home/dohaidang/bigdata_hudi/pipelines/bronze/load_products_bronze.py:1)

Các xử lý chính:
- cast các cột numeric kích thước/trọng lượng sang `int`
- chuẩn hóa `product_category_name`
- thêm metadata ingestion

### Xử lý ở `products_silver_base`

Job:
- [pipelines/silver/load_products_silver_base.py](/home/dohaidang/bigdata_hudi/pipelines/silver/load_products_silver_base.py:1)

Các xử lý chính:
- deduplicate theo `product_id`
- chuẩn hóa `product_category_name`
- `fillna` cho các cột numeric

### Xử lý ở `products_silver`

Job:
- [pipelines/silver/load_products_silver.py](/home/dohaidang/bigdata_hudi/pipelines/silver/load_products_silver.py:1)

Các xử lý chính:
- join `products_silver_base`
- với `product_category_translation_silver`
- sinh `product_category_name_english`

### Vai trò ở `gold`

Bảng này đi vào:
- `category_sales_gold`

## 4.6. `olist_sellers_dataset.csv`

### Cột raw

- `seller_id`
- `seller_zip_code_prefix`
- `seller_city`
- `seller_state`

### Ý nghĩa dữ liệu

Đây là bảng thông tin nhà bán hàng.

### Xử lý ở `sellers_bronze`

Job:
- [pipelines/bronze/load_sellers_bronze.py](/home/dohaidang/bigdata_hudi/pipelines/bronze/load_sellers_bronze.py:1)

Các xử lý chính:
- cast zip sang `string`
- chuẩn hóa city/state
- thêm metadata ingestion

### Xử lý ở `sellers_silver`

Job:
- [pipelines/silver/load_sellers_silver.py](/home/dohaidang/bigdata_hudi/pipelines/silver/load_sellers_silver.py:1)

Các xử lý chính:
- deduplicate theo `seller_id`
- chuẩn hóa lại city/state/zip

### Vai trò downstream

Hiện tại bảng này đã có trong silver, nhưng chưa đi vào 3 bảng gold MVP đầu tiên.

## 4.7. `olist_order_reviews_dataset.csv`

### Cột raw

- `review_id`
- `order_id`
- `review_score`
- `review_comment_title`
- `review_comment_message`
- `review_creation_date`
- `review_answer_timestamp`

### Ý nghĩa dữ liệu

Đây là bảng review đơn hàng. Có text comment, trong đó `review_comment_message` có thể nhiều dòng.

### Xử lý ở `reviews_bronze`

Job:
- [pipelines/bronze/load_reviews_bronze.py](/home/dohaidang/bigdata_hudi/pipelines/bronze/load_reviews_bronze.py:1)

Các xử lý chính:
- đọc CSV với `multiLine=true`
- cast `review_score` sang `int`
- cast 2 cột thời gian sang `timestamp`
- sinh `bronze_record_key`
- thêm metadata ingestion

### Vì sao có `bronze_record_key`

Ở bronze, mục tiêu là giữ gần raw nhất.

Nếu dùng `review_id` làm `record_key` Hudi:
- các dòng review trùng key có thể bị gộp mất

Vì vậy bronze dùng:
- `review_id`
- `order_id`
- `monotonically_increasing_id()`

để tạo key duy nhất cho từng record ingest.

### Xử lý ở `reviews_silver`

Job:
- [pipelines/silver/load_reviews_silver.py](/home/dohaidang/bigdata_hudi/pipelines/silver/load_reviews_silver.py:1)

Các xử lý chính:
- deduplicate theo `review_id`
- trim `review_comment_title`
- trim `review_comment_message`
- cast lại `review_score`

### Vai trò downstream

Hiện tại bảng này đã có trong silver, nhưng chưa đi vào 3 bảng gold MVP đầu tiên.

## 4.8. `olist_geolocation_dataset.csv`

### Cột raw

- `geolocation_zip_code_prefix`
- `geolocation_lat`
- `geolocation_lng`
- `geolocation_city`
- `geolocation_state`

### Ý nghĩa dữ liệu

Đây là bảng lookup địa lý theo zip code.

### Xử lý ở `geolocation_bronze`

Job:
- [pipelines/bronze/load_geolocation_bronze.py](/home/dohaidang/bigdata_hudi/pipelines/bronze/load_geolocation_bronze.py:1)

Các xử lý chính:
- cast zip sang `string`
- cast lat/lng sang `double`
- chuẩn hóa city/state
- sinh `geolocation_key`
- sinh `bronze_record_key`
- thêm metadata ingestion

### Vì sao cần cả `geolocation_key` và `bronze_record_key`

- `geolocation_key`: business key logic
- `bronze_record_key`: key duy nhất để lưu raw record trên Hudi bronze

Điểm quan trọng:
- bronze không muốn mất dòng raw
- silver mới là nơi hợp nhất theo business key

### Xử lý ở `geolocation_silver`

Job:
- [pipelines/silver/load_geolocation_silver.py](/home/dohaidang/bigdata_hudi/pipelines/silver/load_geolocation_silver.py:1)

Các xử lý chính:
- deduplicate theo `geolocation_key`
- chuẩn hóa lại city/state

### Vai trò downstream

Hiện tại bảng này đã có trong silver, nhưng chưa đi vào 3 bảng gold MVP đầu tiên.

## 4.9. `product_category_name_translation.csv`

### Cột raw

- `product_category_name`
- `product_category_name_english`

### Ý nghĩa dữ liệu

Đây là bảng dịch tên category từ tiếng Bồ Đào Nha sang tiếng Anh.

### Xử lý ở `product_category_translation_bronze`

Job:
- [pipelines/bronze/load_product_category_translation_bronze.py](/home/dohaidang/bigdata_hudi/pipelines/bronze/load_product_category_translation_bronze.py:1)

Các xử lý chính:
- chuẩn hóa 2 cột text
- thêm metadata ingestion

### Xử lý ở `product_category_translation_silver`

Job:
- [pipelines/silver/load_product_category_translation_silver.py](/home/dohaidang/bigdata_hudi/pipelines/silver/load_product_category_translation_silver.py:1)

Các xử lý chính:
- deduplicate theo `product_category_name`
- chuẩn hóa text lại

### Vai trò downstream

Bảng này đi vào:
- `products_silver`

## 5. Các cột được sinh thêm theo từng tầng

## 5.1. Cột sinh thêm ở bronze

Các cột metadata chung:
- `_ingested_at`
- `_batch_id`
- `_source_file`
- `_source_system`

Các cột kỹ thuật/phân vùng tiêu biểu:
- `dt`
- `order_item_key`
- `payment_key`
- `bronze_record_key`
- `geolocation_key`

## 5.2. Cột sinh thêm ở silver

Các cột nghiệp vụ/chuẩn hóa tiêu biểu:
- `order_purchase_date`
- `delivery_delay_days`
- `product_category_name_english`

Điểm cần hiểu:
- silver vừa làm sạch dữ liệu, vừa bắt đầu tạo cột phân tích

## 5.3. Cột sinh thêm ở gold

### `daily_sales_gold`

Các cột chính:
- `order_date`
- `order_count`
- `item_count`
- `gross_item_value`
- `gross_freight_value`
- `payment_value`
- `avg_order_payment_value`
- `gold_key`

### `category_sales_gold`

Các cột chính:
- `order_purchase_date`
- `category_name`
- `order_count`
- `item_count`
- `gross_item_value`
- `gross_freight_value`
- `gold_key`

### `customer_ltv_gold`

Các cột chính:
- `customer_id`
- `customer_unique_id`
- `customer_city`
- `customer_state`
- `order_count`
- `lifetime_value`
- `first_order_date`
- `last_order_date`
- `customer_lifespan_days`
- `avg_order_value`
- `ltv_rank`

## 6. Dữ liệu được “vận chuyển” như thế nào

## 6.1. Về mặt logic

Ví dụ với flow `orders`:

1. đọc `olist_orders_dataset.csv`
2. cast và chuẩn hóa ở `orders_bronze`
3. ghi Hudi vào `s3a://lakehouse/bronze/orders_bronze`
4. đọc lại Hudi bronze ở `orders_silver`
5. deduplicate và tạo cột phân tích
6. ghi Hudi vào `s3a://lakehouse/silver/orders_silver`
7. gold jobs đọc `orders_silver` để aggregate

## 6.2. Về mặt kỹ thuật

Luồng kỹ thuật hiện tại:

1. Spark job chạy trong container `spark-master`
2. Spark đọc raw CSV từ workspace
3. Spark ghi Hudi qua `s3a://...`
4. MinIO lưu file data + `.hoodie`
5. Hive Metastore giữ metadata bảng
6. Trino query bảng qua catalog `hive`

## 7. Những điểm quan trọng cần hiểu

## 7.1. Bronze không phải nơi deduplicate business logic mạnh

Đây là nguyên tắc rất quan trọng của project hiện tại.

Ví dụ:
- `reviews_bronze`
- `geolocation_bronze`

được thiết kế để không collapse record gốc khi ghi Hudi.

Deduplicate mạnh được đẩy sang silver.

## 7.2. `record_key` của Hudi ảnh hưởng trực tiếp tới số dòng

Nếu chọn `record_key` quá “business-like” ở bronze:
- dữ liệu raw có thể bị upsert mất dòng

Nếu chọn `record_key` quá ngẫu nhiên ở silver:
- dữ liệu sạch sẽ không hợp nhất đúng entity

Nói ngắn:
- bronze ưu tiên giữ raw
- silver ưu tiên giữ entity đúng

## 7.3. Gold phải giữ đúng granularity

Ví dụ:
- `daily_sales_gold` không join thẳng `payments` với `order_items` ở mức chi tiết
- thay vào đó aggregate từng nhánh về `order_id` trước

Lý do:
- tránh nhân dòng
- tránh làm sai doanh thu

## 8. Tóm tắt lineage ngắn

### Raw -> Bronze

- `olist_orders_dataset.csv` -> `orders_bronze`
- `olist_order_items_dataset.csv` -> `order_items_bronze`
- `olist_customers_dataset.csv` -> `customers_bronze`
- `olist_order_payments_dataset.csv` -> `payments_bronze`
- `olist_products_dataset.csv` -> `products_bronze`
- `olist_sellers_dataset.csv` -> `sellers_bronze`
- `olist_order_reviews_dataset.csv` -> `reviews_bronze`
- `olist_geolocation_dataset.csv` -> `geolocation_bronze`
- `product_category_name_translation.csv` -> `product_category_translation_bronze`

### Bronze -> Silver

- `orders_bronze` -> `orders_silver`
- `order_items_bronze` -> `order_items_silver`
- `customers_bronze` -> `customers_silver`
- `payments_bronze` -> `payments_silver`
- `products_bronze` -> `products_silver_base`
- `sellers_bronze` -> `sellers_silver`
- `reviews_bronze` -> `reviews_silver`
- `geolocation_bronze` -> `geolocation_silver`
- `product_category_translation_bronze` -> `product_category_translation_silver`
- `products_silver_base + product_category_translation_silver` -> `products_silver`

### Silver -> Gold

- `orders_silver + order_items_silver + payments_silver` -> `daily_sales_gold`
- `order_items_silver + products_silver + orders_silver` -> `category_sales_gold`
- `orders_silver + customers_silver + payments_silver` -> `customer_ltv_gold`

## 9. Kết luận ngắn

Nếu nhìn từ góc độ dữ liệu, project này đang làm 3 việc:

- biến raw CSV thành bảng Hudi có thể quản lý được
- biến bảng Hudi thô thành dữ liệu sạch và chuẩn hóa
- biến dữ liệu sạch thành các bảng business có thể query bằng Trino

Khi đọc hoặc sửa code, luôn nên tự hỏi:

- cột này đến từ raw hay được sinh ra sau
- ở tầng này dữ liệu nên “giữ nguyên” hay “làm sạch”
- bảng hiện tại đang ở granularity nào
- downstream nào sẽ dùng kết quả này
