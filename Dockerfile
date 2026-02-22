FROM apache/airflow:2.10.2
USER airflow

# 1. Install Python dependencies for Airflow extraction scripts
COPY requirements.txt /requirements.txt
RUN pip install --no-cache-dir -r /requirements.txt

# 2. Create isolated dbt virtual environment
RUN python -m venv /opt/airflow/dbt_venv && \
    /opt/airflow/dbt_venv/bin/pip install --no-cache-dir dbt-bigquery==1.11.0