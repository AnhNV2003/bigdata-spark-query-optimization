python3 /home/vanh/data/projects/bigdata/src/upload_to_minio.py \
  --source-dir /home/vanh/data/projects/bigdata/dataset/parquet \
  --bucket taxi-data \
  --recursive \
  --from-year 2021 \
  --from-month 1 \
  --to-year 2025 \
  --to-month 11 \
  --skip-existing