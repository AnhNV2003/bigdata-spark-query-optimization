python3 ./src/upload_to_minio.py \
  --bucket taxi-data \
  --endpoint http://localhost:9000 \
  --from-year 2020 \
  --from-month 1 \
  --to-year 2025 \
  --to-month 12 \
  --skip-existing
