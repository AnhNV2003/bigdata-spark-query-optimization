# Tài Liệu Dự Án

Project bám chủ đề **Large-scale GPS Trajectory Processing** trong đề bài
`Distributed Query Optimization on Columnar Storage`.

Ngữ cảnh triển khai:

- Dữ liệu: NYC Yellow Taxi Trip Records năm `2025`.
- Mô hình trajectory: mỗi taxi trip là một trajectory segment theo `origin_zone_id -> destination_zone_id` và thời gian.
- Compute: Spark cluster 1 node (4 workers × 12 cores × 24 GB).
- Storage: MinIO distributed 2 instances trên cùng node (EC:2).
- Mục tiêu: khai phá đặc điểm trajectory, sau đó benchmark format, partitioning, bucketing và join/skew optimization.

## Thứ Tự Chạy (Fresh Setup)

### 1. Tạo virtual environment

```bash
python3.12 -m venv venv
venv/bin/pip install pandas requests pyarrow minio 'pyspark==4.0.1' matplotlib nbconvert ipykernel
```

### 2. Khởi động cluster

```bash
docker compose -f docker-compose.node1.yaml up -d
```

All host-specific values are loaded from `.env`. Start from `.env.example`,
then edit `NODE1_IP`, `NODE2_IP`, `PROJECT_ROOT`, `SPARK_MASTER`,
`SPARK_DRIVER_HOST` and `MINIO_ENDPOINT` for your machine.

### 3. Fix permissions cho Spark log directories

Docker tạo thư mục log với quyền root — cần chmod để Spark process bên trong container write được.

```bash
docker run --rm -v "$(pwd)/service:/service" alpine \
  sh -c "chmod -R 777 /service/spark-master /service/spark-worker-*"

docker compose -f docker-compose.node1.yaml restart \
  spark-master spark-worker-1 spark-worker-2 spark-worker-3 spark-worker-4
```

Verify cluster OK:

```bash
source .env
curl http://$NODE1_IP:$SPARK_MASTER_WEBUI_PORT/json/ | python3 -c "
import json,sys; d=json.load(sys.stdin)
print('Workers:', len(d['workers']), '| Apps:', len(d['activeapps']))
"
```

### 4. Tải reference data

```bash
PYTHONPATH=src venv/bin/python src/download_reference_data.py
```

### 5. Tải taxi parquet về local (nếu chưa có)

```bash
PYTHONPATH=src venv/bin/python src/install_dataset.py
```

### 6. Upload raw data lên MinIO

```bash
bash src/upload_to_minio.sh
```

### 7. Tạo benchmark datasets

```bash
bash src/prepare_benchmark_datasets.sh
```

### 8. Chạy benchmark

```bash
bash src/run_benchmark.sh
```

Script này chạy format, partition, bucketing, join/skew benchmark rồi sinh evidence tự động vào `results/evidence/`.

Nếu chỉ muốn kiểm tra riêng partition pruning với cửa sổ thời gian đúng một tháng:

```bash
bash src/run_partition_window_benchmark.sh
```

Nếu đã có CSV và chỉ muốn tạo lại bảng/charts/evidence:

```bash
bash src/generate_evidence.sh
```

### 9. Xem kết quả

- `results/format/`
- `results/partition/`
- `results/bucketing/`
- `results/join_skew/`
- `results/evidence/`

## Notebooks

Mở Jupyter:

```bash
PYTHONPATH=src venv/bin/jupyter lab --ip=0.0.0.0 --port=8888 --no-browser --ServerApp.token=
```

Thứ tự trình bày:

1. `01_schema_normalization_story.ipynb` — giải thích schema `trips_clean`
2. `02_explore_taxi_data.ipynb` — data quality analysis
3. `03_trajectory_exploration_story.ipynb` — phân tích trajectory đầy đủ
4. `04_benchmark_visualization_story.ipynb` — so sánh kết quả benchmark

## Lưu Ý Kỹ Thuật

**Spark ivy packages (`~/.ivy2`)**

Lần đầu chạy Spark, các packages (`hadoop-aws`, `spark-avro`...) được download tự động vào `~/.ivy2` (~500MB). Nếu muốn download sẵn trước khi mở notebook:

```bash
JAVA_HOME=$HOME/.local/share/jdks/temurin-21 PYTHONPATH=src venv/bin/python -c "
from test_read_minio_parquet import build_spark_session
spark = build_spark_session(app_name='prefetch', endpoint='$MINIO_ENDPOINT', access_key='$MINIO_ACCESS_KEY', secret_key='$MINIO_SECRET_KEY')
print('Packages ready:', spark.version); spark.stop()
"
```

**Java**

PySpark 4.0.1 yêu cầu Java 17+. Notebook tự set `JAVA_HOME` về Temurin 21 (`~/.local/share/jdks/temurin-21`). Khi chạy script từ terminal, cần:

```bash
export JAVA_HOME=$HOME/.local/share/jdks/temurin-21
```

Hoặc các shell script (`run_benchmark.sh`, ...) đã tự detect và set.

## Tài Liệu Chính

- `PROJECT_BRIEF.md`: đề bài gốc.
- `SPARK_SETUP.md`: kiến trúc Spark + MinIO.
- `DATA_DICTIONARY.md`: schema `trips_clean`.
- `benchmark_protocol.md`: kịch bản benchmark.
- `RESULTS_SUMMARY.md`: tổng hợp insight sau benchmark.
