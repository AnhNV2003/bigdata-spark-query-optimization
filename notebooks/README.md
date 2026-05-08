# Notebooks

Thứ tự trình bày khi demo:

1. **`01_schema_normalization_story.ipynb`**
   Giải thích vì sao project cần chốt schema chung `trips_clean`: raw data các năm có schema khác nhau → chuẩn hóa về một schema thống nhất. Hiển thị mapping raw columns → normalized columns và audit cột bị bỏ.

2. **`02_explore_taxi_data.ipynb`**
   Data quality analysis: kiểm tra null rate, outlier (zero distance, negative fare, negative duration), payment type anomaly, self-loop trips. Chứng minh data đủ sạch để benchmark.

3. **`03_trajectory_exploration_story.ipynb`**
   Phân tích đặc điểm trajectory: giải thích zone-based trajectory model, temporal pattern (daily/hourly/weekday), spatial skew (top zones), bucket distribution, top routes, trip duration, peak hour bottleneck. Đây là cơ sở justify các lựa chọn partition, bucketing và join-skew benchmark.

4. **`04_benchmark_visualization_story.ipynb`**
   So sánh kết quả benchmark: format (Parquet/ORC/Avro), partition pruning, hash-bucketed layout, join strategies (SortMerge vs Broadcast vs Salted), shuffle/spill analysis.
