# Hướng Dẫn Benchmark

## Mục đích

Benchmark trong project này dùng để trả lời 4 câu hỏi:

- `Parquet`, `ORC`, `Avro` khác nhau thế nào
- `partitioning` có giúp Spark đọc ít dữ liệu hơn không
- `hash-bucketed layout` có giúp query theo zone không
- `broadcast join` và `salted join` khác baseline ra sao

## Bộ dữ liệu benchmark được tạo như thế nào

Benchmark không chạy trực tiếp trên raw data `2021-2025`. Thay vào đó, project có bước chuẩn bị riêng để tạo dataset benchmark trong `bench/...`.

Chạy:

```bash
bash /home/vanh/data/projects/bigdata/src/prepare_benchmark_datasets.sh
```

Script này sẽ sinh:

- `bench/parquet/raw`
- `bench/orc/raw`
- `bench/avro/raw`
- `bench/parquet/partitioned_year_month`
- `bench/parquet/bucketed_origin_zone_hash`

## Chạy benchmark

```bash
bash /home/vanh/data/projects/bigdata/src/run_benchmark.sh
```

Script sẽ chạy 4 nhóm:

- `format`
- `partition`
- `bucketing`
- `join_skew`

## Cửa sổ benchmark hiện tại

Mặc định benchmark đang dùng:

- `window_start = 2024-01-01 00:00:00`
- `window_end = 2024-02-01 00:00:00`

Điều này có nghĩa là kết quả benchmark hiện tại phản ánh một **benchmark window cố định**, không phải full `2021-2025`.

## Các nhóm query

### Format

- `Q01`
- `Q03`
- `Q06`
- `Q07`

Mục tiêu:

- so scan, aggregate và window query giữa các format

### Partition

- `Q02`
- `Q05`
- `Q06`
- `Q07`

Mục tiêu:

- xem partition pruning có hoạt động không
- xem query theo tháng/ngày có nhanh hơn không

### Bucketing

- `Q11`
- `Q12`

Mục tiêu:

- xem hash-bucketed layout có giúp query tập trung vào một `origin_zone_id` hay không

### Join / Skew

- `Q04`
- `Q08`
- `Q09`
- `Q10`

Mục tiêu:

- xem spatial key có skew không
- so baseline join, broadcast join và salted join

## Kết quả nằm ở đâu

Các file CSV sẽ được ghi vào:

- [results/format](/home/vanh/data/projects/bigdata/results/format)
- [results/partition](/home/vanh/data/projects/bigdata/results/partition)
- [results/bucketing](/home/vanh/data/projects/bigdata/results/bucketing)
- [results/join_skew](/home/vanh/data/projects/bigdata/results/join_skew)

Mỗi file CSV chứa các cột quan trọng như:

- `query_id`
- `query_name`
- `runtime_sec`
- `median_runtime_sec`
- `rows_returned`
- `stages`
- `tasks`
- `uses_broadcast_join`
- `uses_partition_pruning`

Ngoài ra runner còn lưu `EXPLAIN FORMATTED` vào thư mục `plans/`.

## Cách đọc kết quả

- Nếu `uses_partition_pruning = true` và runtime giảm, đó là bằng chứng partitioning có hiệu quả.
- Nếu layout `bucketed_origin_zone_hash` nhanh hơn `raw` trong `Q11`, `Q12`, đó là bằng chứng hash-bucketed layout có ích.
- Nếu `Q09` nhanh hơn `Q08`, broadcast join đang phù hợp với `taxi_zone_dim`.
- Nếu `Q10` không nhanh hơn `Q09`, salted join chưa đáng dùng trong workload hiện tại.
- Nếu `Avro` chậm hơn rõ so với `Parquet` và `ORC`, đó là kết luận cho format benchmark.

## Giới hạn hiện tại

Runner hiện đo tốt:

- runtime
- số stage
- số task
- số dòng trả về
- broadcast join / partition pruning

Runner chưa tự thu đầy đủ:

- `shuffle_read_mb`
- `shuffle_write_mb`
- `spill_mb`

Nếu cần báo cáo sâu hơn, các chỉ số này nên lấy thêm từ Spark UI.
