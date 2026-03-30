# Thiết Lập Hệ Thống

## Mục đích

Tài liệu này giải thích ngắn gọn hệ thống đang chạy như thế nào và bạn cần dùng lệnh gì để làm việc với project.

## Kiến trúc hiện tại

Project dùng 3 lớp chính:

- `Spark`
  là compute layer, dùng để đọc dữ liệu và chạy benchmark
- `MinIO`
  là object storage, nơi chứa raw data và benchmark datasets
- `Jupyter`
  là nơi chạy notebook để khám phá dữ liệu và trực quan hóa kết quả

Các service chính trong [docker-compose.yaml](/home/vanh/data/projects/bigdata/docker-compose.yaml):

- `spark-master`
- `spark-worker`
- `spark-worker-2`
- `minio1`
- `minio2`
- `jupyter`

## Trajectory trong project được hiểu như thế nào

Project không đi theo hướng GPS point-stream. Mỗi dòng taxi trip được xem như một **trajectory segment**:

- bắt đầu ở `origin_zone_id`
- kết thúc ở `destination_zone_id`
- có `pickup_ts`, `dropoff_ts`

Nếu gặp schema cũ có tọa độ GPS trực tiếp, project vẫn giữ thêm:

- `start_lon`, `start_lat`
- `end_lon`, `end_lat`

Nhưng hướng chính thức của project vẫn là:

- **zone + time**

## Khởi động hệ thống

```bash
docker compose up -d
```

Kiểm tra trạng thái:

```bash
docker compose ps
```

## Các giao diện web

- Spark Master: `http://localhost:8080`
- MinIO Console: `http://localhost:9001`
- JupyterLab: `http://localhost:8888`

Spark UI của từng job thường nằm ở:

- `http://localhost:4040`
- hoặc `4041`, `4042` nếu đã có job khác dùng trước

## Dữ liệu nằm ở đâu

Raw data trên MinIO có cấu trúc kiểu:

```text
taxi-data/2021/01/...
taxi-data/2021/02/...
...
taxi-data/2025/11/...
```

Benchmark datasets được tạo dưới các prefix:

```text
taxi-data/bench/parquet/raw/
taxi-data/bench/orc/raw/
taxi-data/bench/avro/raw/
taxi-data/bench/parquet/partitioned_year_month/
taxi-data/bench/parquet/bucketed_origin_zone_hash/
taxi-data/bench/reference/taxi_zone_dim/
```

Kết quả benchmark sẽ được ghi local vào:

- [results](/home/vanh/data/projects/bigdata/results)

## Các script chính

Tạo benchmark datasets:

```bash
bash /home/vanh/data/projects/bigdata/src/prepare_benchmark_datasets.sh
```

Chạy benchmark:

```bash
bash /home/vanh/data/projects/bigdata/src/run_benchmark.sh
```

Build curated cache cho notebook 2:

```bash
bash /home/vanh/data/projects/bigdata/src/build_notebook_cache.sh
```

## Cấu hình S3A tối thiểu

```text
spark.hadoop.fs.s3a.endpoint=http://minio1:9000
spark.hadoop.fs.s3a.access.key=minioadmin
spark.hadoop.fs.s3a.secret.key=minioadmin
spark.hadoop.fs.s3a.path.style.access=true
spark.hadoop.fs.s3a.impl=org.apache.hadoop.fs.s3a.S3AFileSystem
```

## Ghi chú quan trọng

- MinIO là object storage, không phải columnar storage.
- Tính “columnar” của project đến từ format dữ liệu như `Parquet`, `ORC`.
- Benchmark hiện tại mặc định dùng một cửa sổ thời gian cố định để dễ lặp lại và so sánh công bằng.
