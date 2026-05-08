FROM python:3.10-slim

ENV DEBIAN_FRONTEND=noninteractive
ENV PYTHONDONTWRITEBYTECODE=1
ENV PYTHONUNBUFFERED=1
ENV PIP_NO_CACHE_DIR=1
ENV JAVA_HOME=/usr/lib/jvm/java-21-openjdk-amd64
ENV PYSPARK_SUBMIT_ARGS="--packages org.apache.hadoop:hadoop-aws:3.4.1,software.amazon.awssdk:bundle:2.24.6,org.apache.spark:spark-avro_2.13:4.0.1 --conf spark.jars.ivy=/home/jovyan/.ivy2 pyspark-shell"
ARG NB_UID=1005
ARG NB_GID=1005

RUN apt-get update \
    && apt-get install -y --no-install-recommends openjdk-21-jre-headless tini \
    && rm -rf /var/lib/apt/lists/*

RUN groupadd --gid "${NB_GID}" jovyan \
    && useradd --uid "${NB_UID}" --gid "${NB_GID}" -m -s /bin/bash jovyan

RUN pip install \
    ipykernel \
    jupyterlab \
    notebook \
    matplotlib \
    pandas \
    pyarrow \
    pyspark==4.0.1

USER jovyan
WORKDIR /home/jovyan/work

COPY --chown=jovyan:jovyan docker/jupyter_server_config.py /home/jovyan/.jupyter/jupyter_server_config.py

EXPOSE 8888 4040 4041

ENTRYPOINT ["tini", "--"]
CMD ["python", "-m", "jupyter", "lab", "--ip=0.0.0.0", "--port=8888", "--no-browser", "--ServerApp.token=", "--ServerApp.password=", "--ServerApp.allow_origin=*", "--ServerApp.root_dir=/home/jovyan/work", "--ServerApp.default_url=/lab", "--ServerApp.allow_remote_access=True"]
