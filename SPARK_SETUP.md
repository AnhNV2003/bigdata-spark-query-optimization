# Spark Setup

This project uses the Bitnami Spark container image:

- `docker.io/bitnami/spark:4.1`

The cluster is defined in:

- [docker-compose.yaml](/home/vanh/data/projects/bigdata/docker-compose.yaml)

The setup follows the current Bitnami `spark` container documentation pattern:

- `spark`: master node
- `spark-worker`: worker service that can be scaled out

The Compose file is based on Bitnami's published `docker-compose.yml` for Spark, with local adjustments for this project:

- bind-mount the project into `/workspace`
- publish ports `7077` and `8080`
- scale workers with `--scale spark-worker=<n>`

## Start the cluster

From the project root, where `docker-compose.yaml` is located:

```bash
docker compose up -d --scale spark-worker=2
```

Check container status:

```bash
docker compose ps
```

## Web UIs

- Spark Master: `http://localhost:8080`

The master UI will list all worker nodes after startup.

## Mounted paths

The full project is mounted into every container at:

```text
/workspace
```

That means the raw parquet dataset is available inside Spark at:

```text
/workspace/dataset/parquet
```

## Test PySpark in the cluster

Open a PySpark shell from the master service:

```bash
docker compose exec spark /opt/bitnami/spark/bin/pyspark --master spark://spark:7077
```

## Submit a Python job later

When a PySpark script is ready, submit it from the master container:

```bash
docker compose exec spark /opt/bitnami/spark/bin/spark-submit \
  --master spark://spark:7077 \
  /workspace/path/to/job.py
```

## Stop the cluster

```bash
docker compose down
```

## Notes

- This setup is for local development and benchmarking on a trusted machine.
- Spark standalone mode does not enable security features by default.
- Both the master and worker use the same image: `docker.io/bitnami/spark:4.1`
- If you want more or fewer workers, change the scale value:

```bash
docker compose up -d --scale spark-worker=3
```

- Bitnami's Spark config directory inside the container is:

```text
/opt/bitnami/spark/conf
```

## Reference docs

- Bitnami Spark image: https://hub.docker.com/r/bitnami/spark
- Bitnami Spark compose example: https://github.com/bitnami/containers/blob/main/bitnami/spark/docker-compose.yml
