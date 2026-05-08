# Hướng Dẫn Benchmark

Benchmark phục vụ chủ đề **Large-scale GPS Trajectory Processing** và mặc định chỉ dùng dữ liệu taxi năm `2025`.

## Flow

1. Upload raw NYC taxi parquet lên MinIO.
2. Profile dữ liệu để hiểu phân bố thời gian, zone, route và skew.
3. Dựa trên profile, tạo layout benchmark.
4. Chạy benchmark format, partition, bucketing và join/skew.
5. Tổng hợp insight vào `RESULTS_SUMMARY.md`.

## 1. Profile Dữ Liệu

```bash
bash src/profile_trajectory_data.sh
```

Output:

```text
results/profile/overview.csv
results/profile/monthly_counts.csv
results/profile/trajectory_mode_counts.csv
results/profile/hourly_counts.csv
results/profile/top_origin_zones.csv
results/profile/top_destination_zones.csv
results/profile/top_routes.csv
results/profile/origin_bucket_distribution.csv
results/profile/recommendations.md
```

Mục tiêu:

- biết dữ liệu tập trung ở tháng/năm nào để justify partitioning
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
