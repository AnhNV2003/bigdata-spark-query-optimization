# Live Benchmark Rerun Status

Generated at: `2026-05-14 06:49:00 UTC`

## Completed

- Created local ignored `venv/` with `python3.14` because no project venv existed and `python3.12` was not available.
- Installed `requirements.txt` successfully, including `pandas`, `matplotlib`, `pyarrow`, `pyspark==4.0.1`, `nbconvert`, and `ipykernel`.
- Ran unit tests with `PYTHONPATH=src venv/bin/python -m unittest discover -s tests`: `4 tests OK`.
- Generated evidence from existing CSV results with `bash src/generate_evidence.sh`.
- Wrote evidence to `results/evidence/` and updated `docs/RESULTS_SUMMARY.md`.

## Live Rerun Attempt

Attempted command:

```bash
bash src/run_partition_window_benchmark.sh
```

Result: failed before reading benchmark data.

The full `bash src/run_benchmark.sh` rerun was not started after this failure because it uses the same unreachable Spark master and MinIO endpoints and would fail before executing the benchmark groups.

Observed Spark error:

```text
Failed to connect to master 127.0.0.1:7077
All masters are unresponsive! Giving up.
```

Container and network observations:

- `docker compose -f docker-compose.node1.yaml up -d` started the Spark and MinIO containers.
- `curl http://127.0.0.1:8080/json/` failed with connection refused.
- `curl http://127.0.0.1:9000/minio/health/ready` failed with connection refused.
- Restarting with `NODE1_IP=192.168.1.43` also left host ports unreachable from macOS.
- `minio1` entered a restart loop with `Unable to configure server grid RPC services` because the current compose file runs two MinIO peers on the same host/IP in distributed mode.
- The failed local compose stack was stopped after the rerun attempt.

Root cause:

The checked-in `docker-compose.node1.yaml` relies on `network_mode: host` and distributed MinIO endpoint assumptions that are not valid in this macOS/Docker Desktop environment. The existing CSVs were likely produced on the intended Linux/networked setup. Live Spark rerun needs either the original host setup or a local Docker Compose variant with explicit port mappings and standalone MinIO.

## Evidence Available

- `results/evidence/RESULTS_SUMMARY.generated.md`
- `results/evidence/TOPIC5_REQUIREMENT_AUDIT.md`
- `results/evidence/VALIDATION_REPORT.md`
- `results/evidence/*.csv`
- `results/evidence/charts/*.png`
