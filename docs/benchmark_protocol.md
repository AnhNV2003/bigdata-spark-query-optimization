# Hướng Dẫn Benchmark

Benchmark phục vụ chủ đề **Large-scale GPS Trajectory Processing** và mặc định chỉ dùng dữ liệu taxi năm `2025`.

## Flow

1. Upload raw NYC taxi parquet lên MinIO.
2. Dùng notebooks để hiểu data quality, trajectory pattern, route/zone skew và bucket distribution.
3. Dựa trên notebook analysis, tạo layout benchmark.
4. Chạy benchmark format, partition, bucketing và join/skew.
5. Sinh evidence vào `results/evidence/` và đồng bộ `docs/RESULTS_SUMMARY.md`.

## 1. Data Understanding Bằng Notebooks

Project không dùng script profile riêng nữa. Phần data understanding được thay bằng notebooks để dễ demo và giải thích trực quan:

- `notebooks/02_explore_taxi_data.ipynb`: kiểm tra data quality, null rate, outlier, negative fare/duration, payment anomaly và self-loop trips.
- `notebooks/03_trajectory_exploration_story.ipynb`: phân tích trajectory model, temporal pattern, top zones, spatial skew, top routes, peak-hour bottleneck và hash bucket distribution.

Mục tiêu:

- biết dữ liệu đủ sạch để benchmark
- biết pattern theo thời gian để justify partitioning
- biết zone/route nào skew để justify join-skew benchmark
- kiểm tra bucket distribution trước khi dùng hash bucket theo `origin_zone_id`

## 2. Tạo Benchmark Dataset

```bash
bash src/prepare_benchmark_datasets.sh
```

Sinh:

```text
bench/parquet/raw
bench/orc/raw
bench/avro/raw
bench/parquet/partitioned_year_month
bench/parquet/bucketed_origin_zone_hash
bench/reference/taxi_zone_dim
```

## 3. Chạy Benchmark

```bash
bash src/run_benchmark.sh
```

Script này cũng chạy `src/generate_evidence.py` ở cuối để tạo:

```text
results/evidence/RESULTS_SUMMARY.generated.md
results/evidence/TOPIC5_REQUIREMENT_AUDIT.md
results/evidence/VALIDATION_REPORT.md
results/evidence/*.csv
results/evidence/charts/*.png
```

Nếu chỉ cần sinh lại evidence từ CSV đã có:

```bash
bash src/generate_evidence.sh
```

Nếu chỉ cần rerun partition benchmark với cửa sổ một tháng rõ ràng:

```bash
bash src/run_partition_window_benchmark.sh
```

Nhóm query:

- `format`: Q01, Q03, Q06, Q07
- `partition`: Q02, Q05, Q06, Q07
- `bucket_layout`: Q11, Q12
- `join_skew`: Q04, Q08, Q09, Q10

## 4. Cách Đọc

- `Parquet`/`ORC` tốt hơn `Avro` nếu scan/aggregate/window query nhanh hơn.
- `partitioned_year_month` tốt nếu query time-window giảm runtime và plan có partition pruning.
- `bucketed_origin_zone_hash` tốt nếu lookup/join theo một `origin_zone_id` nhanh hơn raw.
- `broadcast join` tốt nếu dimension `taxi_zone_dim` nhỏ.
- `salted join` chỉ đáng dùng nếu skew theo `origin_zone_id` đủ lớn và runtime giảm.

## 5. Kết Quả

CSV benchmark nằm trong:

```text
results/format/
results/partition/
results/bucketing/
results/join_skew/
```

`EXPLAIN FORMATTED` nằm trong thư mục `plans/` cạnh từng file kết quả.

## 6. Lưu Ý Diễn Giải

- Không claim partitioning luôn nhanh hơn nếu CSV hiện tại không chứng minh điều đó.
- `PartitionFilters` trong physical plan chứng minh Spark có partition pruning, còn runtime có thể vẫn chậm hơn nếu overhead metadata, shuffle, sort hoặc window function lớn hơn phần scan tiết kiệm được.
- Layout `bucketed_origin_zone_hash` là hash-partitioned directory layout theo `origin_zone_bucket`; nó chứng minh data skipping theo spatial key, nhưng không phải Spark catalog `bucketBy` để loại bỏ shuffle join.
- `Broadcast join` là chiến lược đúng cho `taxi_zone_dim` nhỏ. `Salted join` nên được giải thích là kỹ thuật xử lý skew cho trường hợp không thể broadcast dimension lớn.
