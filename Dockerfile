FROM apache/airflow:2.10.2
USER airflow
RUN python -m venv /opt/airflow/dbt_venv && \
    /opt/airflow/dbt_venv/bin/pip install --no-cache-dir dbt-bigquery==1.11.0