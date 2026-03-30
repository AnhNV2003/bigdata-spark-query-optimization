# Tóm Tắt Kết Quả

## Mục đích

Tài liệu này chỉ giữ lại các insight chính, để người đọc hiểu nhanh benchmark hiện tại đang nói gì mà không cần mở từng file CSV.

## 1. Format benchmark

Kết luận chính:

- `Parquet` và `ORC` đang khá sát nhau trên workload trajectory analytics
- `Avro` chậm hơn rõ rệt
- với bài toán hiện tại, `Parquet` hoặc `ORC` hợp lý hơn `Avro`

## 2. Partition benchmark

Kết luận chính:

- layout `partitioned_year_month` đang cho thấy partition pruning hoạt động
- query theo tháng/ngày hưởng lợi rõ hơn khi filter theo `trip_year`, `trip_month`

## 3. Join / skew benchmark

Kết luận chính:

- `broadcast join` đang tốt hơn baseline join với `taxi_zone_dim` nhỏ
- `salted join` hiện chưa cho lợi ích rõ trên workload đang đo
- `origin_zone_id` là một khóa hợp lý để quan sát skew không gian

## 4. Hash-bucketed layout benchmark

Kết luận chính:

- layout `bucketed_origin_zone_hash` đang nhanh hơn `raw` ở các query nhắm vào một `origin_zone_id` cụ thể
- điều này cho thấy hash-bucketed layout có ích cho workload lookup/join theo zone

## 5. Kết luận ngắn gọn

Nếu chỉ cần nhớ vài ý:

- `Parquet` và `ORC` phù hợp hơn `Avro`
- `partitioning` theo thời gian là có ích
- `broadcast join` đang là lựa chọn join tốt nhất với dimension nhỏ
- `hash-bucketed layout` có ích với query tập trung theo zone

## 6. Giới hạn hiện tại

- benchmark hiện tại chạy trên một benchmark window cố định, không phải toàn bộ `2021-2025`
- một số chỉ số sâu như `shuffle`, `spill` vẫn cần lấy thêm từ Spark UI nếu muốn báo cáo chi tiết hơn
