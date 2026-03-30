# Data Model

## Mục đích

Project dùng một lớp dữ liệu chung để tránh việc mỗi năm lại phải viết query theo một schema khác nhau. Lớp đó là `trips_clean`.

## 1. `trips_clean` là gì

`trips_clean` là view chuẩn hóa dùng chung cho toàn bộ notebook và benchmark.

Ý tưởng:

- raw data các năm không hoàn toàn giống nhau
- project map chúng về một schema logic thống nhất
- từ đó mọi query chỉ cần chạy trên `trips_clean`

## 2. Các cột quan trọng nhất trong `trips_clean`

### Nhóm thời gian

- `pickup_ts`
  thời điểm bắt đầu chuyến đi
- `dropoff_ts`
  thời điểm kết thúc chuyến đi
- `trip_date`
  ngày của chuyến đi
- `trip_year`
  năm của chuyến đi
- `trip_month`
  tháng của chuyến đi

### Nhóm không gian

- `origin_zone_id`
  zone đón khách
- `destination_zone_id`
  zone trả khách
- `route_key`
  tuyến theo dạng `origin_zone_id -> destination_zone_id`
- `origin_zone_bucket`
  bucket hash của `origin_zone_id`, dùng cho benchmark hash-bucketed layout

### Nhóm nghiệp vụ

- `vendor_name_norm`
  tên hãng taxi đã chuẩn hóa
- `payment_type_norm`
  loại thanh toán đã chuẩn hóa
- `passenger_count`
  số hành khách
- `trip_distance`
  quãng đường chuyến đi
- `fare_amt`
  tiền cước cơ bản
- `tip_amt`
  tiền tip
- `tolls_amt`
  phí cầu đường
- `total_amt`
  tổng tiền chuyến đi

### Nhóm GPS trực tiếp

- `start_lon`, `start_lat`
- `end_lon`, `end_lat`
- `has_gps_coordinates`

Các cột này chủ yếu có ý nghĩa khi gặp schema cũ có tọa độ trực tiếp.

### Nhóm mô tả schema

- `trajectory_mode`
  cho biết dòng đó đang ở dạng `zone_based`, `coordinate_based` hay `unknown`
- `schema_family`
  cho biết dữ liệu nghiêng về schema mới hay schema cũ

## 3. Các bảng phụ

### `taxi_zone_dim`

Bảng tra cứu zone, dùng để biến `LocationID` thành thông tin dễ hiểu:

- `location_id`
- `borough`
- `zone`
- `service_zone`

Đây là bảng rất quan trọng cho trajectory analysis vì nó giúp đổi:

- `origin_zone_id = 132`

thành:

- một khu vực thật trong New York

### `payment_type_dim`

Bảng phụ nhỏ để giải nghĩa `payment_type_norm`.

Nó không phải bảng quan trọng nhất của project, nhưng hữu ích cho:

- kết quả dễ đọc hơn
- benchmark join với dimension nhỏ

## 4. Cách hiểu trajectory trong project này

Project không đi theo GPS point-stream. Mỗi dòng taxi trip được xem là một **trajectory segment**:

- bắt đầu ở `origin_zone_id`
- kết thúc ở `destination_zone_id`
- có `pickup_ts` và `dropoff_ts`

Nếu có GPS trực tiếp thì đó là phần bổ sung, không phải hướng chính.

## 5. Nếu cần nhớ ít thôi

Chỉ cần nhớ 3 thứ:

- `trips_clean` là bảng chuẩn hóa chung
- `taxi_zone_dim` là bảng đổi `zone_id` thành tên khu vực thật
- `route_key` là cách project biểu diễn tuyến đi từ zone này sang zone khác
