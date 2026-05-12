# Tóm Tắt Kết Quả

## Mục đích

Tài liệu này tổng hợp kết quả benchmark trên dữ liệu NYC taxi trajectory năm 2025 (~48.7 triệu trips).

Trước khi đọc kết quả benchmark, nên xem notebooks data understanding để hiểu đặc điểm dữ liệu:

- `notebooks/02_explore_taxi_data.ipynb`: data quality, null rate, outlier và anomaly.
- `notebooks/03_trajectory_exploration_story.ipynb`: phân bố theo thời gian, zone, route, spatial skew và bucket distribution.

## 1. Format benchmark

Kết quả (median runtime, 3 measured runs mỗi query):

| Query | Parquet | ORC | Avro |
|-------|---------|-----|------|
| Q01 projection scan | 1.03s | 0.68s | 8.31s |
| Q03 vendor/zone agg | 1.24s | 1.06s | 8.87s |
| Q06 route ranking | 1.11s | 0.77s | 8.95s |
| Q07 OD volume | 0.53s | 0.50s | 8.45s |

Kết luận:

- Parquet nhanh hơn Avro **~9.8x**, ORC nhanh hơn Avro **~12.3x**
- ORC nhỉnh hơn Parquet nhẹ trên workload này
- Cả Parquet và ORC đều phù hợp cho trajectory analytics

Giải thích:

- **Avro là row-oriented**: mỗi record được serialize nguyên khối. Dù query chỉ cần 5 cột (Q01), Avro phải deserialize toàn bộ 20+ cột trong mỗi record. Với 48.7M rows, lượng I/O thừa rất lớn.
- **Parquet và ORC là columnar**: chỉ đọc column cần thiết (column projection). Ngoài ra chúng lưu column statistics (min/max, null count) trong mỗi row group/stripe, cho phép skip toàn bộ row group nếu filter không khớp.
- **ORC nhỉnh hơn Parquet nhẹ** trên workload này do ORC dùng lightweight compression mặc định (ZLIB) và có row-level index tốt hơn cho các phép filter, nhưng sự khác biệt không lớn.
- **File size phản ánh efficiency**: Parquet raw = 1.04 GB, ORC raw = 0.81 GB, Avro raw = 2.10 GB. ORC nén tốt nhất, Avro lớn nhất.

## 2. Partition benchmark

So sánh raw layout vs `partitioned_year_month`:

| Query | Raw | Partitioned | Speedup |
|-------|-----|------------|---------|
| Q02 time filter | 0.52s | 0.36s | 30% |
| Q05 daily zone flow | 0.65s | 0.46s | 29% |
| Q06 route ranking | 0.88s | 0.91s | -3% |
| Q07 OD volume | 0.49s | 0.39s | 20% |

Kết luận:

- Partition pruning hoạt động: EXPLAIN plan xác nhận `PartitionFilters: [trip_year = 2025, trip_month = 1]`
- Q02, Q05, Q07 nhanh hơn 20-30% nhờ Spark skip các partition không liên quan
- Q06 không cải thiện do overhead từ nhiều partition files nhỏ và window function phức tạp
- Partitioning theo thời gian là lựa chọn hợp lý cho trajectory queries

Giải thích:

- **Partition pruning**: khi dữ liệu được tổ chức thành thư mục `trip_year=2025/trip_month=1/`, Spark đọc metadata thư mục trước khi mở file. Nếu query có filter `trip_year = 2025 AND trip_month = 1`, Spark chỉ đọc 1/12 số thư mục, bỏ qua 11 tháng còn lại mà không cần mở file nào.
- **Raw layout**: tất cả file nằm chung một thư mục, Spark phải mở và scan toàn bộ dù query chỉ cần 1 tháng.
- **Q06 không cải thiện** vì: (1) window function `DENSE_RANK()` yêu cầu shuffle toàn bộ dữ liệu đã filter, chi phí shuffle lớn hơn lợi ích pruning; (2) layout partitioned tạo nhiều file nhỏ, Spark phải list và mở nhiều file hơn, gây overhead metadata.
- **Benchmark này dùng window 1 năm** (2025-01 đến 2025-12), nên tất cả 12 partition đều match filter. Với window nhỏ hơn (ví dụ 1 tháng), speedup sẽ lớn hơn đáng kể.

## 3. Hash-bucketed layout benchmark

So sánh raw vs `bucketed_origin_zone_hash` (16 buckets theo `origin_zone_id`):

| Query | Raw | Bucketed | Speedup |
|-------|-----|----------|---------|
| Q11 zone lookup | 1.32s | 0.76s | 43% |
| Q12 zone join | 0.97s | 0.55s | 44% |

Kết luận:

- Hash-bucketed layout nhanh hơn ~43% cho query nhắm vào một `origin_zone_id` cụ thể
- Layout này có ích cho workload lookup/join theo spatial key

Giải thích:

- **Hash bucketing** chia dữ liệu thành 16 thư mục dựa trên `PMOD(HASH(origin_zone_id), 16)`. Mỗi `origin_zone_id` luôn nằm trong cùng một bucket.
- Khi query filter `origin_zone_bucket = X AND origin_zone_id = Y`, Spark chỉ đọc 1/16 dữ liệu (1 bucket) thay vì scan toàn bộ. Đây là dạng **data skipping theo spatial key**.
- **Raw layout** buộc Spark scan toàn bộ 48.7M rows rồi mới filter, dẫn đến I/O gấp 16x so với bucketed.
- Bucketing đặc biệt có ích khi join 2 bảng cùng bucketed theo cùng key — Spark có thể làm **sort-merge join không cần shuffle** (bucket join). Trong benchmark này, `taxi_zone_dim` quá nhỏ nên Spark chọn broadcast join, nhưng với dimension lớn hơn, bucket join sẽ phát huy tác dụng.

## 4. Join / skew benchmark

So sánh các join strategy với `taxi_zone_dim` (265 rows):

| Query | Strategy | Runtime | Shuffle | Spill |
|-------|----------|---------|---------|-------|
| Q04 | Skew aggregation | 1.25s | 0.5 MB | 0 MB |
| Q08 | SortMerge join | 5.27s | 25.9 MB | 743 MB |
| Q09 | Broadcast join | 1.15s | 0.9 MB | 0 MB |
| Q10 | Salted join | 4.02s | 0.9 MB | 0 MB |

Kết luận:

- **Broadcast join nhanh hơn SortMerge 4.6x** — loại bỏ hoàn toàn shuffle và spill
- SortMerge join gây **743 MB disk spill** dù dimension chỉ có 265 rows — bottleneck rõ rệt
- Salted join giảm shuffle nhưng overhead salt key khiến không nhanh bằng broadcast
- Với dimension nhỏ, broadcast join là chiến lược tối ưu nhất

Giải thích:

- **SortMerge join (Q08)** yêu cầu cả 2 bảng được shuffle theo join key (`origin_zone_id`), rồi sort cả 2 bên trước khi merge. Với 48.7M rows bên trái, shuffle di chuyển 25.9 MB qua network và sort gây spill 743 MB ra disk khi executor memory không đủ. Đây là bottleneck lớn nhất.
- **Broadcast join (Q09)** copy toàn bộ `taxi_zone_dim` (265 rows, vài KB) ra tất cả executors. Mỗi executor join locally mà không cần shuffle hay sort bảng lớn. Kết quả: gần như không có network overhead.
- **Salted join (Q10)** thêm salt key ngẫu nhiên vào cả 2 bảng để phân tán rows từ zone có skew (ví dụ zone 237 chiếm 4.4% trips) ra nhiều partition. Tuy giảm được skew, nhưng: (1) phải CROSS JOIN dimension bảng nhỏ với 8 salt values, tạo ra 8x rows phía dimension; (2) join key dài hơn (`origin_zone_id + salt_key`). Overhead này lớn hơn lợi ích khi dimension đã đủ nhỏ cho broadcast.
- **Khi nào salted join có ích**: khi dimension table quá lớn để broadcast (hàng triệu rows, vượt `spark.sql.autoBroadcastJoinThreshold` = 10 MB mặc định) và join key có skew nghiêm trọng. Trong trường hợp đó, SortMerge join sẽ bị một vài partition chứa phần lớn data (skewed partition), và salted join phân tán tải đều hơn.

## 5. Adaptive Query Execution (AQE)

Spark 4.0 bật AQE mặc định (`spark.sql.adaptive.enabled = true`). AQE tối ưu query plan **tại runtime** dựa trên statistics thực tế thay vì chỉ dùng statistics tĩnh trước khi chạy.

Các tối ưu AQE thực hiện tự động trong benchmark này:

- **Coalesce shuffle partitions**: AQE gộp các shuffle partition nhỏ lại, giảm số task và overhead scheduling. Mặc định Spark có 200 shuffle partitions — AQE tự giảm xuống nếu data nhỏ.
- **Convert SortMerge join thành broadcast join**: nếu tại runtime, AQE phát hiện một bên join đủ nhỏ, nó tự chuyển từ SortMerge sang broadcast. Trong benchmark Q09, hint `BROADCAST(z)` ép broadcast từ đầu; nhưng ngay cả không có hint, AQE sẽ tự phát hiện `taxi_zone_dim` nhỏ và chuyển đổi.
- **Skew join optimization**: AQE phát hiện partition bị skew (partition chứa data lớn hơn median nhiều lần) và tự chia nhỏ partition đó. Đây là phiên bản tự động của salted join.

Tất cả benchmark runs trong project này đều có AQE bật. Physical plan có tag `AdaptiveSparkPlan` xác nhận AQE đang hoạt động.

## 6. Kết luận ngắn gọn

1. **Columnar format**: Parquet/ORC nhanh hơn Avro 10-12x cho analytical workload, nhờ column projection và column statistics giúp skip data không cần đọc.
2. **Partitioning**: Partition theo thời gian giảm 20-30% runtime nhờ Spark skip toàn bộ thư mục partition không khớp filter.
3. **Bucketing**: Hash-bucketed layout giảm ~43% runtime nhờ data skipping theo spatial key, đặc biệt có ích cho zone-targeted queries.
4. **Join optimization**: Broadcast join là lựa chọn tốt nhất cho dimension nhỏ — loại bỏ shuffle/spill hoàn toàn. SortMerge join chỉ phù hợp khi cả 2 bảng đều lớn.
5. **AQE**: Spark 4.0 tự động tối ưu join strategy, shuffle partitions và skew handling tại runtime.

## 7. Giới hạn hiện tại

- Benchmark chạy trên dữ liệu năm 2025 (48.7M rows, ~1 GB Parquet)
- Cluster: 4 workers x 12 cores trên 1 node — tổng 48 cores, ~96 GB memory
- Shuffle và spill metrics thu thập qua Spark REST API
- Chưa benchmark AQE on vs off riêng biệt (AQE luôn bật mặc định)
- Với dữ liệu petabyte-scale, các tối ưu partition pruning và bucketing sẽ cho speedup lớn hơn nhiều do lượng data skip tỷ lệ thuận với tổng data
