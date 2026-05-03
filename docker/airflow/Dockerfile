FROM apache/airflow:3.0.0

USER root

RUN apt-get update && \
    apt-get install -y --no-install-recommends docker.io && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/*

USER airflow

COPY docker/airflow/requirements.txt /requirements.txt

RUN pip install --no-cache-dir -r /requirements.txt
