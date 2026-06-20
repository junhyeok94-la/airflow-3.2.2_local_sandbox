FROM apache/airflow:3.2.2-python3.12

USER root

# 시스템 패키지 업데이트
RUN apt-get update && apt-get install -y --no-install-recommends \
    build-essential \
    libpq-dev \
    git \
    && rm -rf /var/lib/apt/lists/*

USER airflow

# Airflow 3.2 프로바이더 및 Task SDK 설치
RUN pip install --no-cache-dir \
    apache-airflow-providers-postgres \
    apache-airflow-providers-openai \
    apache-airflow-providers-common-sql \
    apache-airflow-task-sdk \
    psycopg2-binary \
    pandas \
    sqlalchemy \
    requests \
    dbt-core~=1.10.0 \
    dbt-clickhouse~=1.10.0 \
    clickhouse-connect \
    astronomer-cosmos

WORKDIR /opt/airflow
