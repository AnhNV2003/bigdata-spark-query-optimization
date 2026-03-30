# Tài Liệu Dự Án

Thư mục `docs/` chứa bộ tài liệu ngắn gọn để người mới có thể hiểu nhanh project này đang làm gì, chạy như thế nào và benchmark ra sao.

## Project này là gì

Đây là project Spark theo hướng:

- **Distributed Query Optimization on Columnar Storage**
- ngữ cảnh ứng dụng là **Taxi GPS Trajectory Processing**

Ý tưởng chính:

- dữ liệu gốc là NYC Taxi Trip Records
- dữ liệu được lưu trên MinIO
- Spark dùng để đọc, chuẩn hóa và benchmark
- project so sánh `Parquet`, `ORC`, `Avro`
- project thử `partitioning`, `hash-bucketed layout` và `join optimization`

## Nên đọc theo thứ tự nào

1. [PROJECT_BRIEF.md](/home/vanh/data/projects/bigdata/docs/PROJECT_BRIEF.md)
2. [SPARK_SETUP.md](/home/vanh/data/projects/bigdata/docs/SPARK_SETUP.md)
3. [DATA_DICTIONARY.md](/home/vanh/data/projects/bigdata/docs/DATA_DICTIONARY.md)
4. [benchmark_protocol.md](/home/vanh/data/projects/bigdata/docs/benchmark_protocol.md)
5. [RESULTS_SUMMARY.md](/home/vanh/data/projects/bigdata/docs/RESULTS_SUMMARY.md)

## Nếu muốn chạy từ đầu tới cuối

Đây là thứ tự thực tế để một người mới vào repo có thể dựng dữ liệu, chạy benchmark và xem kết quả.

1. Khởi động hệ thống:

```bash
docker compose up -d --build
```

2. Nếu raw data chưa có trên MinIO, upload dữ liệu lên MinIO:

```bash
bash /home/vanh/data/projects/bigdata/src/upload_to_minio.sh
```

3. Tạo curated cache cho notebook khám phá dữ liệu:

```bash
bash /home/vanh/data/projects/bigdata/src/build_notebook_cache.sh
```

4. Tạo benchmark datasets trong `bench/...`:

```bash
bash /home/vanh/data/projects/bigdata/src/prepare_benchmark_datasets.sh
```

5. Chạy benchmark:

```bash
bash /home/vanh/data/projects/bigdata/src/run_benchmark.sh
```

6. Mở notebook để xem dữ liệu và kết quả:

- [01_schema_normalization_story.ipynb](/home/vanh/data/projects/bigdata/notebooks/01_schema_normalization_story.ipynb)
- [02_explore_taxi_data.ipynb](/home/vanh/data/projects/bigdata/notebooks/02_explore_taxi_data.ipynb)
- [03_trajectory_exploration_story.ipynb](/home/vanh/data/projects/bigdata/notebooks/03_trajectory_exploration_story.ipynb)
- [04_benchmark_visualization_story.ipynb](/home/vanh/data/projects/bigdata/notebooks/04_benchmark_visualization_story.ipynb)

7. Xem kết quả benchmark đã ghi ra:

- [results/format](/home/vanh/data/projects/bigdata/results/format)
- [results/partition](/home/vanh/data/projects/bigdata/results/partition)
- [results/bucketing](/home/vanh/data/projects/bigdata/results/bucketing)
- [results/join_skew](/home/vanh/data/projects/bigdata/results/join_skew)

## Bộ tài liệu hiện tại

- [PROJECT_BRIEF.md](/home/vanh/data/projects/bigdata/docs/PROJECT_BRIEF.md)
  Mô tả đề bài gốc bằng tiếng Anh, giữ sát wording của tài liệu.

- [SPARK_SETUP.md](/home/vanh/data/projects/bigdata/docs/SPARK_SETUP.md)
  Giải thích kiến trúc hệ thống, cách khởi động và các script chính.

- [DATA_DICTIONARY.md](/home/vanh/data/projects/bigdata/docs/DATA_DICTIONARY.md)
  Giải thích các bảng và cột quan trọng như `trips_clean`, `taxi_zone_dim`.

- [benchmark_protocol.md](/home/vanh/data/projects/bigdata/docs/benchmark_protocol.md)
  Hướng dẫn cách chuẩn bị benchmark dataset, chạy benchmark và đọc kết quả.

- [RESULTS_SUMMARY.md](/home/vanh/data/projects/bigdata/docs/RESULTS_SUMMARY.md)
  Tóm tắt các kết quả benchmark đã có và insight chính.

## Nguồn code quan trọng

- [trajectory_utils.py](/home/vanh/data/projects/bigdata/src/trajectory_utils.py)
  Chuẩn hóa dữ liệu thành `trips_clean`.

- [prepare_benchmark_datasets.py](/home/vanh/data/projects/bigdata/src/prepare_benchmark_datasets.py)
  Sinh các dataset benchmark trong `bench/...`.

- [benchmark_definitions.py](/home/vanh/data/projects/bigdata/src/benchmark_definitions.py)
  Định nghĩa các query benchmark.

- [run_benchmark.py](/home/vanh/data/projects/bigdata/src/run_benchmark.py)
  Chạy benchmark và ghi file kết quả.
