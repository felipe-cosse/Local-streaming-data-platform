FROM apache/airflow:3.3.0-python3.12

USER root
RUN apt-get update \
    && apt-get install -y --no-install-recommends default-libmysqlclient-dev build-essential pkg-config \
    && rm -rf /var/lib/apt/lists/*

USER airflow
COPY docker/airflow/requirements.txt /tmp/requirements.txt
RUN pip install --no-cache-dir -r /tmp/requirements.txt
