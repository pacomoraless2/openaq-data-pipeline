import os
from airflow import DAG, Dataset
from airflow.operators.empty import EmptyOperator
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import (
    GCSToBigQueryOperator,
)
from datetime import datetime, timedelta

# --- ENVIRONMENT VARIABLES ---
PROJECT_ID = os.environ["AIRFLOW_VAR_GCP_PROJECT_ID"]
DATASET_RAW = os.environ["AIRFLOW_VAR_BQ_DATASET_RAW"]
BUCKET_NAME = os.environ["AIRFLOW_VAR_GCS_BUCKET_NAME"]

# --- PATH VARIABLES ---
BASE_DIR_JSON_LOCS = os.environ["AIRFLOW_VAR_LOCATIONS_JSON_PATH"]
BASE_DIR_JSON_MEAS = os.environ["AIRFLOW_VAR_MEASUREMENTS_JSON_PATH"]

# --- DATASETS ---
bronze_ready_dataset = Dataset(f"bigquery://{PROJECT_ID}/{DATASET_RAW}/bronze_ready")

default_args = {
    "owner": "data_engineering",
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    dag_id="99_recover_datalake_to_bq",
    default_args=default_args,
    start_date=datetime(2025, 1, 1),
    schedule=None, 
    catchup=False,
    tags=["elt", "disaster-recovery", "bronze"],
    doc_md="""
    # Disaster Recovery DAG
    This DAG truncates the Raw tables in BigQuery and fully reconstructs them 
    by reading the entire historical data available in the data lake, 
    eliminating the need to re-fetch data from the source API.
    """,
) as dag:

    start = EmptyOperator(task_id="start")

    # =========================================================================
    # RECOVERY: LOCATIONS
    # =========================================================================
    
    recover_locations = GCSToBigQueryOperator(
        task_id="recover_locations_to_bq",
        bucket=BUCKET_NAME,
        source_objects=[f"{BASE_DIR_JSON_LOCS}/*"], 
        destination_project_dataset_table=f"{PROJECT_ID}.{DATASET_RAW}.raw_locations",
        source_format="NEWLINE_DELIMITED_JSON",
        write_disposition="WRITE_TRUNCATE", # <-- THE MAGIC: Drops current data and reloads from scratch
        create_disposition="CREATE_IF_NEEDED",
        ignore_unknown_values=True,
        time_partitioning={"type": "DAY", "field": "_audit_logical_date"},
        autodetect=False,
        schema_fields=[
            {"name": "data", "type": "JSON", "mode": "NULLABLE"},
            {"name": "_audit_run_id", "type": "STRING", "mode": "NULLABLE"},
            {"name": "_audit_logical_date", "type": "DATE", "mode": "NULLABLE"},
            {"name": "_audit_extracted_at", "type": "TIMESTAMP", "mode": "NULLABLE"},
            {"name": "_audit_source", "type": "STRING", "mode": "NULLABLE"},
            {"name": "_audit_gcs_filename", "type": "STRING", "mode": "NULLABLE"},
        ],
    )

    # =========================================================================
    # RECOVERY: MEASUREMENTS
    # =========================================================================

    recover_measurements = GCSToBigQueryOperator(
        task_id="recover_measurements_to_bq",
        bucket=BUCKET_NAME,
        source_objects=[f"{BASE_DIR_JSON_MEAS}/*"],
        destination_project_dataset_table=f"{PROJECT_ID}.{DATASET_RAW}.raw_measurements",
        source_format="NEWLINE_DELIMITED_JSON",
        write_disposition="WRITE_TRUNCATE", # <-- THE MAGIC
        create_disposition="CREATE_IF_NEEDED",
        ignore_unknown_values=True,
        time_partitioning={"type": "DAY", "field": "_audit_logical_date"},
        cluster_fields=["_audit_sensor_id", "_audit_extracted_at"],
        autodetect=False,
        schema_fields=[
            {"name": "data", "type": "JSON", "mode": "NULLABLE"},
            {"name": "_audit_run_id", "type": "STRING", "mode": "NULLABLE"},
            {"name": "_audit_sensor_id", "type": "INTEGER", "mode": "NULLABLE"},
            {"name": "_audit_logical_date", "type": "DATE", "mode": "NULLABLE"},
            {"name": "_audit_extracted_at", "type": "TIMESTAMP", "mode": "NULLABLE"},
            {"name": "_audit_gcs_filename", "type": "STRING", "mode": "NULLABLE"},
        ],
    )

    emit_dataset_trigger = EmptyOperator(
        task_id="trigger_dbt_models",
        outlets=[bronze_ready_dataset],
    )

    # Parallel orchestration: Load both tables simultaneously, then emit the dataset trigger
    start >> [recover_locations, recover_measurements] >> emit_dataset_trigger