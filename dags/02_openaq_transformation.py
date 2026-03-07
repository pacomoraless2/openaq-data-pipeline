import os
from airflow import DAG, Dataset
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta

# --- ENVIRONMENT VARIABLES ---
PROJECT_ID = os.environ["AIRFLOW_VAR_GCP_PROJECT_ID"]
DATASET_RAW = os.environ["AIRFLOW_VAR_BQ_DATASET_RAW"]
GCP_LOCATION = os.environ["AIRFLOW_VAR_GCP_LOCATION"]
GCP_KEY_PATH = os.environ["AIRFLOW_VAR_GCP_KEY_PATH"]

# --- DATASET TRIGGERS ---
# Unified dataset representing the Bronze layer is fully updated and ready
bronze_ready_dataset = Dataset(f"bigquery://{PROJECT_ID}/{DATASET_RAW}/bronze_ready")

default_args = {
    "owner": "data_engineering",
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    dag_id="02_openaq_transformation",
    max_active_runs=1,
    default_args=default_args,
    start_date=datetime(2026, 1, 1),
    # Trigger DAG only when the ingestion DAG signals it is completely done
    schedule=[bronze_ready_dataset],
    catchup=False,
    tags=["elt", "transformation", "silver", "dbt"],
) as dag:

    run_dbt_models_task = BashOperator(
        task_id="run_dbt_models",
        bash_command=(
            "source /opt/airflow/dbt_venv/bin/activate && "
            "cd /opt/airflow/openaq_transform && "
            "dbt deps && "
            "dbt source freshness && "
            "dbt build --profiles-dir ."
        ),
        env={
            "DBT_GCP_PROJECT": PROJECT_ID,
            "DBT_BQ_DATASET": DATASET_RAW,
            "DBT_GCP_LOCATION": GCP_LOCATION,
            "GOOGLE_APPLICATION_CREDENTIALS": GCP_KEY_PATH,
        },
        append_env=True,
    )
