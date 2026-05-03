# Airflow Trong Dự Án Này

## 1. Airflow dùng để làm gì

Trong project này, Airflow là lớp `orchestration`.

Nó không thay thế Spark, Hudi, MinIO hay Trino.
Vai trò của nó là:

- sắp xếp thứ tự chạy các job
- quản lý dependency giữa các tầng `bronze -> silver -> gold`
- cho phép rerun một phần pipeline khi lỗi
- ghi log chạy theo từng task
- cung cấp giao diện để quan sát toàn bộ pipeline

Nói ngắn:

- `Spark` làm ETL
- `Hudi` lưu bảng dữ liệu
- `MinIO` là object storage
- `Hive Metastore` giữ metadata
- `Trino` query
- `Airflow` điều phối toàn bộ quá trình

## 2. Vì sao dự án này cần Airflow

Trước khi có Airflow, flow chạy chủ yếu bằng tay:

- chạy bronze jobs
- chạy silver jobs
- chạy gold jobs
- chạy verify
- query lại Trino

Cách này ổn cho giai đoạn dựng ban đầu, nhưng có các vấn đề:

- dễ quên thứ tự chạy
- khó biết job nào fail trong chuỗi dài
- rerun thủ công mất thời gian
- không có một giao diện tập trung để nhìn toàn bộ pipeline

Airflow giải quyết đúng các vấn đề đó.

## 3. Airflow đang điều phối gì trong dự án

Hiện tại tôi đã thêm DAG:

- [dags/hudi_pipeline_dag.py](/home/dohaidang/bigdata_hudi/dags/hudi_pipeline_dag.py:1)

DAG này điều phối toàn bộ flow Hudi hiện tại:

1. chạy toàn bộ `bronze`
2. khi bronze xong thì chạy `silver`
3. khi silver xong thì chạy `gold`
4. chạy verify Hudi end-to-end
5. chạy data quality checks trên Hudi
6. chạy freshness và reconciliation checks trên Hudi
7. chạy smoke query trên Trino

## 4. Execution model cụ thể

Đây là điểm quan trọng nhất cần hiểu.

Airflow trong project này **không trực tiếp chạy logic Spark bên trong DAG file**.
Thay vào đó nó dùng `BashOperator` để gọi các script runtime đã được kiểm chứng trước đó.

Flow thực tế là:

1. `airflow-scheduler` trigger task
2. task chạy `bash` trong container Airflow
3. bash gọi script ở `scripts/`
4. script dùng `docker exec` vào container `spark-master` hoặc `trino`
5. container đích chạy job thật

Lý do chọn cách này:

- runtime Hudi đã được chốt ổn trong `spark-master`
- tránh phải tạo thêm một runtime Spark/Hudi thứ hai trong Airflow
- dùng lại đúng wrapper đã verify

## 5. Các thay đổi hạ tầng để Airflow làm được việc này

### 5.1. `docker/airflow/Dockerfile`

File:
- [docker/airflow/Dockerfile](/home/dohaidang/bigdata_hudi/docker/airflow/Dockerfile:1)

Đã được bổ sung:
- cài `docker.io` vào image Airflow

Ý nghĩa:
- task trong Airflow có thể gọi `docker exec`

### 5.2. `docker-compose.yml`

File:
- [docker-compose.yml](/home/dohaidang/bigdata_hudi/docker-compose.yml:1)

Đã được bổ sung vào phần `airflow-common`:

- mount `docker.sock`
- `group_add` để Airflow container có quyền nói chuyện với Docker socket
- cấu hình auth manager của Airflow 3 theo `SimpleAuthManager`
- mount `./jars` vào `/opt/airflow/jars`
- chạy Airflow containers với `user: "0:0"` trong local stack hiện tại để tránh lỗi quyền với `docker.sock` và file auth
- file password cố định cho `SimpleAuthManager`

Ý nghĩa:
- Airflow container có thể điều phối lại `spark-master` và `trino`
- Airflow 3 có thể đăng nhập bằng credential đơn giản từ biến môi trường

## 5.3. Điều chỉnh theo Airflow 3

Đây là chỗ rất quan trọng.

Airflow 3 khác Airflow 2 ở 2 điểm mà project này đã đụng ngay:

- không còn CLI `airflow users create`
- command `webserver` được thay bằng `api-server`

Vì vậy stack đã được chỉnh như sau:

- `airflow-init` chỉ chạy `airflow db migrate`
- user đăng nhập được quản lý bởi `SimpleAuthManager`
- `airflow-webserver` chạy `api-server`
- thêm service `airflow-dag-processor`

Điều này bám đúng CLI của image `apache/airflow:3.0.0`.

Lưu ý:

- đây là lựa chọn thực dụng cho local development
- nếu sau này muốn siết chặt security hơn, có thể quay lại mô hình non-root sau khi xử lý dứt điểm quyền socket, log volume, config files, và execution API auth

## 5.4. Vì sao `admin/admin` trước đó không đăng nhập được

Đây là bẫy cấu hình dễ gặp của Airflow 3.

`SimpleAuthManager` không hiểu biến `simple_auth_manager_users` theo kiểu:

- `username:password`

Mà nó hiểu theo kiểu:

- `username:role`

Ví dụ:

- `admin:admin`

được hiểu là:

- username = `admin`
- role = `admin`

chứ không phải password = `admin`

Nếu không cấu hình file password riêng, Airflow sẽ tự sinh password ngẫu nhiên và in ra log lúc startup.

Vì vậy stack hiện tại đã được sửa để dùng file:

- `/opt/airflow/configs/airflow/simple_auth_manager_passwords.json`

với nội dung mặc định:

```json
{"admin":"admin"}
```

## 5.5. Vì sao task có thể fail trước khi có attempt log

Airflow 3 có thêm một lớp nội bộ gọi là `execution API`.

Trong stack nhiều container như project này:

- `scheduler` không chạy task trực tiếp ngay
- nó gọi `execution API` trên `api-server`
- sau đó mới spawn process thực thi task

Nếu `execution_api_server_url` trỏ sai, ví dụ để mặc định:

```text
http://localhost:8080/execution/
```

thì trong container `scheduler`, `localhost` là chính nó, không phải `airflow-webserver`.

Kết quả là:

- scheduler báo `ConnectError: [Errno 111] Connection refused`
- task fail khi còn ở trạng thái `queued`
- file `attempt=*.log` có thể rỗng vì subprocess task chưa được tạo

Vì vậy stack hiện tại đã được chỉnh rõ:

```text
AIRFLOW__CORE__EXECUTION_API_SERVER_URL=http://airflow-webserver:8080/execution/
```

## 5.6. Vì sao scheduler có thể bị `Invalid auth token: Signature verification failed`

Airflow 3 dùng JWT để `scheduler` gọi `execution API` trên `api-server`.

Nếu mỗi container tự sinh một `jwt_secret` khác nhau, sẽ xảy ra tình huống:

- `scheduler` ký token bằng secret A
- `webserver` verify token bằng secret B
- kết quả là `403 Forbidden`

Log thường thấy:

```text
Invalid auth token: Signature verification failed
```

Khi đó task sẽ fail rất sớm, thường vẫn ở trạng thái `queued`, và file `attempt=*.log` có thể rỗng.

Vì vậy stack hiện tại đã được chỉnh để mọi container dùng cùng một biến:

```text
AIRFLOW__API_AUTH__JWT_SECRET
```

## 5.7. Vì sao `BashOperator` có thể báo `TemplateNotFound`

`BashOperator` của Airflow có cơ chế render template cho trường `bash_command`.

Nếu command kết thúc bằng một đường dẫn `.sh`, ví dụ:

```text
bash /opt/airflow/scripts/run_trino_gold_checks.sh
```

Airflow có thể hiểu nhầm đây là template file cần load từ thư mục DAGs, và sinh lỗi:

```text
TemplateNotFound
```

Để tránh việc đó, DAG hiện tại dùng dạng:

```text
set -e; bash /opt/airflow/scripts/run_trino_gold_checks.sh
```

Khi đó Airflow xem đây là shell command bình thường, không cố resolve `.sh` như template file.

## 6. Các script Airflow đang dùng

### 6.1. Spark wrapper

File:
- [scripts/spark_submit_container.sh](/home/dohaidang/bigdata_hudi/scripts/spark_submit_container.sh:1)

Vai trò:
- chạy job Python trong container `spark-master`
- nạp đúng jars Hudi/Hadoop/AWS
- đảm bảo job dùng runtime Spark 3.5.8 đã chốt

### 6.2. Trino check script

File:
- [scripts/run_trino_gold_checks.sh](/home/dohaidang/bigdata_hudi/scripts/run_trino_gold_checks.sh:1)

Vai trò:
- kiểm tra `SHOW TABLES FROM hive.analytics`
- kiểm tra row count của 3 bảng gold

## 7. Cấu trúc DAG hiện tại

### 7.1. Tên DAG

- `hudi_full_pipeline`

### 7.2. Các nhóm task

#### `bronze`

Bao gồm các task:

- `orders_bronze`
- `order_items_bronze`
- `customers_bronze`
- `payments_bronze`
- `products_bronze`
- `sellers_bronze`
- `reviews_bronze`
- `geolocation_bronze`
- `product_category_translation_bronze`

Mỗi task gọi:

```bash
bash /opt/airflow/scripts/spark_submit_container.sh <bronze_job.py> --output-format hudi
```

#### `silver`

Bao gồm các task:

- `orders_silver`
- `order_items_silver`
- `customers_silver`
- `payments_silver`
- `products_silver_base`
- `sellers_silver`
- `reviews_silver`
- `geolocation_silver`
- `product_category_translation_silver`
- `products_silver`

Mỗi task gọi:

```bash
bash /opt/airflow/scripts/spark_submit_container.sh <silver_job.py> --input-format hudi --output-format hudi
```

#### `gold`

Bao gồm:

- `daily_sales_gold`
- `category_sales_gold`
- `customer_ltv_gold`

Mỗi task gọi:

```bash
bash /opt/airflow/scripts/spark_submit_container.sh <gold_job.py> --input-format hudi --output-format hudi
```

#### `validation`

Hai task cuối:

- `verify_hudi_pipeline`
- `verify_trino_gold`

`verify_hudi_pipeline` chạy:

```bash
bash /opt/airflow/scripts/spark_submit_container.sh pipelines/tools/verify_hudi_pipeline.py
```

`verify_trino_gold` chạy:

```bash
bash /opt/airflow/scripts/run_trino_gold_checks.sh
```

## 8. Dependency giữa các task

Đây là phần quan trọng nhất về nghiệp vụ.

### 8.1. Bronze -> Silver

Từng bảng silver phụ thuộc đúng bảng bronze tương ứng:

- `orders_bronze -> orders_silver`
- `order_items_bronze -> order_items_silver`
- `customers_bronze -> customers_silver`
- `payments_bronze -> payments_silver`
- `products_bronze -> products_silver_base`
- `sellers_bronze -> sellers_silver`
- `reviews_bronze -> reviews_silver`
- `geolocation_bronze -> geolocation_silver`
- `product_category_translation_bronze -> product_category_translation_silver`

### 8.2. Silver derived dependency

`products_silver` phụ thuộc vào cả:

- `products_silver_base`
- `product_category_translation_silver`

Nghĩa là chỉ khi cả 2 xong thì `products_silver` mới được chạy.

### 8.3. Silver -> Gold

- `daily_sales_gold` phụ thuộc:
  - `orders_silver`
  - `order_items_silver`
  - `payments_silver`

- `category_sales_gold` phụ thuộc:
  - `order_items_silver`
  - `products_silver`
  - `orders_silver`

- `customer_ltv_gold` phụ thuộc:
  - `orders_silver`
  - `customers_silver`
  - `payments_silver`

### 8.4. Validation

Chỉ khi cả 3 bảng gold xong thì:

- `verify_hudi_pipeline` mới chạy
- sau đó `verify_trino_gold`

Điều này đảm bảo:
- verify không đọc phải output nửa chừng
- Trino chỉ bị kiểm khi Hudi side đã xong

## 9. Airflow khác gì so với chạy script tay

### Chạy tay

Bạn phải tự nhớ:

1. bronze trước
2. silver sau
3. products_silver phụ thuộc translation
4. gold chỉ chạy khi silver đủ
5. verify cuối

### Chạy bằng Airflow

Airflow nhớ toàn bộ dependency đó thay bạn.

Bạn chỉ cần:

- trigger DAG
- theo dõi task nào chạy, task nào fail
- rerun lại đúng task cần thiết

## 10. Các lợi ích cụ thể trong dự án này

### 10.1. Quan sát pipeline

Trong UI Airflow, bạn nhìn thấy:

- toàn bộ DAG
- task nào đang chạy
- task nào fail
- task nào phụ thuộc task nào

### 10.2. Rerun có chọn lọc

Ví dụ:

- nếu `reviews_bronze` fail, bạn không cần chạy lại cả pipeline
- chỉ cần fix và rerun đúng task đó

### 10.3. Log tập trung

Logs của từng task nằm trong Airflow logs, thay vì bạn phải giữ nhiều terminal riêng.

### 10.4. Chuẩn bị cho production-like workflow

Dù đây là local project, Airflow giúp pipeline có hình dạng gần production hơn:

- có DAG
- có scheduling
- có retry
- có dependency graph

## 11. Cách khởi động Airflow cho project này

### 11.1. Rebuild Airflow image

Vì Dockerfile của Airflow đã đổi, cần rebuild:

```bash
docker compose build airflow-init airflow-webserver airflow-dag-processor airflow-scheduler
```

Lưu ý:

- không nên export `AIRFLOW_UID=$(id -u)` cho stack này
- image Airflow đang được chốt chạy bằng user `50000`
- nếu ép sang UID host như `1000`, Airflow có thể fail với lỗi `uid not found`

### 11.2. Khởi động Airflow

```bash
docker compose up -d airflow-init airflow-webserver airflow-dag-processor airflow-scheduler
```

### 11.3. Truy cập UI

UI:

- `http://localhost:8080`

Credential mặc định hiện tại:

- username: `admin`
- password: `admin`

trừ khi bạn đã override bằng biến môi trường:

- `AIRFLOW_ADMIN_USERNAME`
- `AIRFLOW_ADMIN_PASSWORD`

Credential này được đọc từ:

- `AIRFLOW__CORE__SIMPLE_AUTH_MANAGER_USERS`
- `AIRFLOW__CORE__SIMPLE_AUTH_MANAGER_PASSWORDS_FILE`

## 12. Cách chạy DAG

### Từ UI

1. mở Airflow UI
2. tìm DAG `hudi_full_pipeline`
3. bật DAG nếu đang paused
4. trigger run

### Từ CLI

```bash
docker exec airflow-webserver airflow dags trigger hudi_full_pipeline
```

## 13. Cách đọc một run của DAG

Một run thành công nghĩa là:

1. toàn bộ bronze tasks pass
2. toàn bộ silver tasks pass
3. toàn bộ gold tasks pass
4. `verify_hudi_pipeline` pass
5. `verify_trino_gold` pass

Nếu fail ở một task:

- xem log task đó trước
- xác định lỗi nằm ở:
  - code transform
  - runtime Spark/Hudi
  - Docker/service availability
  - Trino query layer

## 14. Những giả định hiện tại của DAG

DAG hiện tại giả định rằng các service sau đã tồn tại và có thể dùng được:

- `minio`
- `minio-init`
- `metastore-postgres`
- `hive-metastore`
- `spark-master`
- `trino`
- `airflow-dag-processor`

Nói cách khác:
- Airflow đang orchestration pipeline jobs
- chưa orchestration việc boot toàn bộ hạ tầng Docker từ đầu

Điều này là hợp lý cho giai đoạn hiện tại.

## 15. Những gì Airflow chưa làm trong dự án

Hiện tại DAG chưa làm các việc sau:

- chưa tự bootstrap Docker stack
- chưa materialize Trino DDL
- chưa có branching theo mode `full refresh` / `incremental`
- chưa có alerting email/slack
- chưa có dataset quality checks nâng cao

Đó là các bước mở rộng sau.

## 16. Định hướng tiếp theo sau Airflow

Sau khi DAG này chạy ổn, bước tiếp theo hợp lý là:

1. thêm DAG validate riêng
2. thêm DAG incremental hoặc backfill
3. thêm data quality checks
4. thêm dashboard/demo query layer

## 17. Kết luận ngắn

Trong dự án này, Airflow là lớp điều phối cho pipeline Hudi đã có sẵn.

Nó không xử lý dữ liệu trực tiếp, mà:

- gọi đúng các Spark jobs
- giữ đúng dependency giữa các tầng
- chạy verify cuối pipeline
- cho bạn một giao diện vận hành rõ ràng hơn nhiều so với chạy script tay

Nói ngắn:

- trước đây bạn có các script chạy được
- bây giờ Airflow biến đống script đó thành một pipeline có orchestration thật
