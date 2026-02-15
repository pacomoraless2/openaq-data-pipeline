FROM apache/airflow:2.10.2
USER airflow
RUN pip install --no-cache-dir requests dbt-bigquery==1.8.0
RUN pip install apache-airflow-providers-google
