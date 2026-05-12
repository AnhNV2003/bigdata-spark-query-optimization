. "$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)/env.sh"

"$PYTHON_BIN" "$PROJECT_ROOT/src/upload_to_minio.py" \
  --bucket "${MINIO_BUCKET:-taxi-data}" \
  --endpoint "${MINIO_ENDPOINT:-http://${NODE1_IP:-127.0.0.1}:${MINIO_API_PORT:-9000}}" \
  --from-year "${UPLOAD_FROM_YEAR:-2020}" \
  --from-month "${UPLOAD_FROM_MONTH:-1}" \
  --to-year "${UPLOAD_TO_YEAR:-2025}" \
  --to-month "${UPLOAD_TO_MONTH:-12}" \
  --skip-existing
