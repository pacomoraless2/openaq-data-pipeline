import os
from airflow import DAG, Dataset
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta


# --- GLOBAL CONFIGURATION ---
# Using Airflow Variables with defaults ensures code portability across environments (Dev/Prod)
PROJECT_ID = os.getenv("AIRFLOW_VAR_GCP_PROJECT_ID", "data-platform-project-485710")
DATASET_RAW = os.getenv("AIRFLOW_VAR_BQ_DATASET_RAW", "openaq_raw")


# Listen for this Dataset to be updated
raw_measurements_dataset = Dataset(
    f"bigquery://{PROJECT_ID}/{DATASET_RAW}/raw_measurements"
)

default_args = {
    "owner": "data_engineering",
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    dag_id="02_openaq_transformation",
    default_args=default_args,
    start_date=datetime(2023, 1, 1),
    schedule=[raw_measurements_dataset],  # DATA-AWARE SCHEDULING
    catchup=False,
    tags=["elt", "transformation", "silver", "dbt"],
) as dag:

    run_dbt_models_task = BashOperator(
        task_id="run_dbt_models",
        bash_command=(
            "source /opt/airflow/dbt_venv/bin/activate && "
            "cd /opt/airflow/openaq_transform && "
            "dbt deps && "
            "dbt build --profiles-dir ."
        ),
    )
