# Hudi Runtime Notes

Thư mục này dùng để giữ các cấu hình hoặc ghi chú liên quan tới Apache Hudi.

Hiện tại pipeline đã được chuyển theo hướng `Hudi-first`, nhưng để chạy thật bằng Spark bạn vẫn cần có đủ jars trong thư mục `jars/`.

## Các loại jars thường cần

Tối thiểu bạn nên có:
- `hudi-spark3.5-bundle_2.12-<version>.jar`
- `hadoop-aws-<version>.jar`
- `aws-java-sdk-bundle-<version>.jar`

Với stack hiện tại của repo này, bộ jars đã chốt là:
- `hudi-spark3.5-bundle_2.12-1.1.1.jar`
- `hadoop-aws-3.3.4.jar`
- `aws-java-sdk-bundle-1.12.262.jar`

## Vì sao cần các jars này

- `hudi-spark3.5-bundle`:
  - cung cấp datasource `format("hudi")`
  - cung cấp Spark SQL extension của Hudi

- `hadoop-aws` và `aws-java-sdk-bundle`:
  - giúp Spark đọc và ghi `s3a://...`
  - cần cho MinIO vì project đang dùng đường storage `s3a://lakehouse/...`

## Cách code hiện tại sử dụng jars

Helper [pipelines/common/spark_session.py](/home/dohaidang/bigdata_hudi/pipelines/common/spark_session.py:1) sẽ tự quét:
- `./jars`
- `/opt/spark/work-dir/jars`

Nếu phát hiện jar Hudi, helper sẽ tự bật:
- `spark.serializer=org.apache.spark.serializer.KryoSerializer`

Wrapper [scripts/spark_submit_container.sh](/home/dohaidang/bigdata_hudi/scripts/spark_submit_container.sh:1) là nơi nạp classpath thật cho Spark container bằng `--jars` và `extraClassPath`.

## Lưu ý

- Nếu chưa có jars, code vẫn có thể chạy ở nhánh `parquet`
- Khi muốn chạy Hudi thật, hãy đảm bảo jars tương thích với:
  - Spark `3.5.x`
  - Scala `2.12`
  - Hadoop `3.3.4` của image `spark:3.5.8-python3`

## Cảnh báo quan trọng về runtime

Máy local của bạn hiện có `spark-submit` host ở version `4.1.1`, trong khi bộ jars của project được chốt cho:
- Spark `3.5.8`
- Scala `2.12.18`

Vì vậy:
- không nên chạy `spark-submit ...` trực tiếp từ host nếu nó trỏ vào Spark `4.1.1`
- nên chạy qua container `spark-master` của project
- với pipeline hiện tại, nên ưu tiên đọc/ghi Hudi theo `path` bằng `format("hudi")`, không phụ thuộc `HoodieCatalog`

Các script hỗ trợ:
- [scripts/spark_submit_container.sh](/home/dohaidang/bigdata_hudi/scripts/spark_submit_container.sh:1)
- [scripts/smoke_test_hudi_orders.sh](/home/dohaidang/bigdata_hudi/scripts/smoke_test_hudi_orders.sh:1)

Ví dụ:

```bash
bash scripts/spark_submit_container.sh pipelines/bronze/load_orders_bronze.py
```

Smoke test nhanh:

```bash
bash scripts/smoke_test_hudi_orders.sh
```

## Cách tải nhanh

Repo đã có sẵn script:
- [scripts/download_hudi_jars.sh](/home/dohaidang/bigdata_hudi/scripts/download_hudi_jars.sh:1)

Chạy từ root project:

```bash
bash scripts/download_hudi_jars.sh
```
