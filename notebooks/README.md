# Notebook Scenarios

Thư mục này chứa các notebook theo đúng flow của project: chốt schema chung trước, sau đó mới khai phá trên dữ liệu đã chuẩn hóa. Với dữ liệu lớn, notebook mặc định chỉ đọc một lát cắt có kiểm soát từ curated cache để phù hợp với máy cá nhân.

## Danh sách notebook

- [01_schema_normalization_story.ipynb](/home/vanh/data/projects/bigdata/notebooks/01_schema_normalization_story.ipynb)
  Notebook đầu tiên để chốt schema chung `trips_clean` và giải thích raw schema được chuẩn hóa như thế nào.

- [02_explore_taxi_data.ipynb](/home/vanh/data/projects/bigdata/notebooks/02_explore_taxi_data.ipynb)
  Notebook khai phá trên `trips_clean` theo workflow curated-first. Nếu full curated cache đã có, notebook sẽ đọc một lát cắt đã partition từ cache đó. Nếu full cache chưa có, notebook sẽ tự dựng một exploration cache nhỏ đúng theo `trip_year` và `trip_month` đang chọn, để vẫn chạy ổn trên máy cá nhân.

- [03_trajectory_exploration_story.ipynb](/home/vanh/data/projects/bigdata/notebooks/03_trajectory_exploration_story.ipynb)
  Kịch bản phân tích trajectory theo zone, route, thời gian và payment/vendor.

- [04_benchmark_visualization_story.ipynb](/home/vanh/data/projects/bigdata/notebooks/04_benchmark_visualization_story.ipynb)
  Kịch bản trực quan hóa kết quả benchmark từ các file CSV trong `results/`.

## Gợi ý trình bày khi demo

1. Mở `01_schema_normalization_story.ipynb` để giải thích vì sao project phải chốt `trips_clean` trước.
2. Mở `02_explore_taxi_data.ipynb` để khai phá một lát cắt đại diện từ curated cache trên schema chung, rồi mở rộng phạm vi nếu cần.
3. Mở `03_trajectory_exploration_story.ipynb` để kể câu chuyện GPS trajectory processing theo zone + time.
4. Mở `04_benchmark_visualization_story.ipynb` để so sánh format, partitioning, hash-bucketed layout và join optimization.
