# Thiết Lập Hệ Thống

## Kiến Trúc Tổng Quan

```
┌──────────────────────────────────────────────────────────────────────┐
│  Node 1 (100.127.42.127) — Primary compute + storage               │
│                                                                      │
│  ┌──────────────┐  ┌──────────────────────────────────────────────┐  │
│  │ Spark Master │  │            Spark Workers                     │  │
│  │  :7077       │  │  worker-1 :7081   12 cores / 24 GB          │  │
│  │  UI :8080    │  │  worker-2 :7082   12 cores / 24 GB          │  │
│  └──────┬───────┘  │  worker-3 :7083   12 cores / 24 GB          │  │
│         │          │  worker-4 :7084   12 cores / 24 GB          │  │
│         │          └──────────────────────────────────────────────┘  │
│         │                          │                                 │
│         │          ┌───────────────▼──────────────┐                  │
│         │          │ MinIO Object Storage         │                  │
│         │          │  API :9100  Console :9101    │                  │
│         │          │  Bucket: taxi-data           │                  │
│         │          │   ├── 2025/         (raw)    │                  │
│         │          │   ├── bench/     (layouts)   │                  │
│         │          │   └── notebook_cache/        │                  │
│         │          └─────────────────────────────┘                  │
│         │                                                            │
│  ┌──────▼──────────────────────────────────────┐                    │
│  │ Driver (host Python process)                │                    │
│  │  submit jobs via spark-submit / PySpark     │                    │
│  │  Spark UI :4040 (per application)           │                    │
│  └─────────────────────────────────────────────┘                    │
│                                                                      │
│  ┌───────────────────┐                                              │
│  │ Jupyter Lab :8888 │  (Docker container, mounts /workspace)       │
│  └───────────────────┘                                              │
└──────────────────────────────────────────────────────────────────────┘

┌──────────────────────────────────────────────────────────────────────┐
│  Node 2 (100.113.89.12) — Secondary (optional, via Tailscale)       │
│                                                                      │
│  ┌────────────────────┐  ┌────────────────────────────┐             │
│  │ Spark Worker       │  │ MinIO (distributed peer)   │             │
│  │  :7084  6c / 6 GB  │  │  :9100                     │             │
│  └────────────────────┘  └────────────────────────────┘             │
└──────────────────────────────────────────────────────────────────────┘
        ▲                           ▲
        │      Tailscale VPN        │
        └───────────────────────────┘
```

## Data Flow

```
NYC TLC Website                MinIO (taxi-data)              Spark Cluster
     │                              │                              │
     │  install_dataset.py          │                              │
     ├─────────────────────►  dataset/parquet/                     │
     │                         (local disk)                        │
     │                              │                              │
     │                  upload_to_minio.sh                         │
     │                   ──────────►│  2025/                       │
     │                              │   ├── 01/                    │
     │                              │   ├── ...                    │
     │                              │   └── 12/                    │
     │                              │                              │
     │              prepare_benchmark_datasets.sh                  │
     │                              │◄─────────────────────────────┤
     │                              │  bench/parquet/raw           │
     │                              │  bench/orc/raw               │
     │                              │  bench/avro/raw              │
     │                              │  bench/parquet/partitioned   │
     │                              │  bench/parquet/bucketed      │
     │                              │                              │
     │                       run_benchmark.sh                      │
     │                              │◄─────────────────────────────┤
     │                              │              results/*.csv ──┤──► results/
     │                              │              plans/*.txt  ───┤──► results/*/plans/
```

## Cấu Hình Hiện Tại

### Compute

Node 1 chạy toàn bộ compute:

| Component | Cores | Memory | Port |
|-----------|-------|--------|------|
| spark-master | - | - | 7077 (RPC), 8080 (UI) |
| spark-worker-1 | 12 | 24 GB | 7081, 8081 |
| spark-worker-2 | 12 | 24 GB | 7082, 8082 |
| spark-worker-3 | 12 | 24 GB | 7083, 8083 |
| spark-worker-4 | 12 | 24 GB | 7084, 8084 |
| **Tổng** | **48** | **96 GB** | |

### Storage

MinIO chạy **standalone mode** trên node 1.

**Tại sao standalone thay vì distributed?**

Project được thiết kế cho 2-node distributed MinIO (`docker-compose.yaml` có `http://minio{1...2}:9100/data`). Tuy nhiên, node 2 (100.113.89.12) không phải lúc nào cũng online vì kết nối qua Tailscale VPN phụ thuộc vào mạng. Để đảm bảo benchmark reproducible và không bị gián đoạn, production config (`docker-compose.node1.yaml`) dùng MinIO standalone trên node 1.

MinIO standalone vẫn là S3-compatible object storage — Spark truy cập qua `s3a://` protocol giống hệt distributed mode. Sự khác biệt chỉ ở replication (standalone không replicate data sang node khác), không ảnh hưởng đến benchmark query performance.

**Cách bật distributed mode** (khi node 2 online):

1. Trên node 1: dùng `docker-compose.yaml` thay vì `docker-compose.node1.yaml`
2. Trên node 2: `docker compose -f docker-compose.node2.yaml up -d`
3. MinIO tự erasure-code data giữa 2 node

### Endpoint

```text
Spark master:  spark://100.127.42.127:7077
Spark UI:      http://100.127.42.127:8080
MinIO API:     http://100.127.42.127:9000
MinIO Console: http://100.127.42.127:9001  (minio1)
               http://100.127.42.127:9003  (minio2)
Jupyter Lab:   http://100.127.42.127:8888
```

## Ý Nghĩa Với Trajectory Processing

- **Spark** là compute layer chạy query/benchmark trên trajectory segment. Driver process chạy trên host, submit jobs tới cluster qua `spark://` protocol.
- **MinIO** là distributed object storage cho raw taxi data và benchmark datasets. Spark đọc/ghi qua `s3a://` giống Amazon S3.
- **Columnar optimization** đến từ file format (Parquet, ORC) và data layout (partitioning, bucketing), không phải từ storage layer. MinIO chỉ serve bytes — mọi data skipping xảy ra ở Spark khi đọc column metadata và partition directories.

## Lệnh Kiểm Tra

```bash
# Cluster status
docker compose -f docker-compose.node1.yaml ps
curl http://100.127.42.127:8080/json/

# MinIO health
curl -I http://100.127.42.127:9100/minio/health/ready

# Node 2 (khi online)
ssh vanh@100.113.89.12 'cd /home/vanh/data/projects/bigdata && docker compose -f docker-compose.node2.yaml ps'
curl -I http://100.113.89.12:9100/minio/health/ready
```
