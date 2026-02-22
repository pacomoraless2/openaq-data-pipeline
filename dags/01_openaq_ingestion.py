import os
from airflow import DAG, Dataset
from airflow.operators.bash import BashOperator
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import (
    GCSToBigQueryOperator,
)
from datetime import datetime, timedelta

# --- ENVIRONMENT VARIABLES (Strict Mode: Fails instantly if missing) ---
PROJECT_ID = os.environ["AIRFLOW_VAR_GCP_PROJECT_ID"]
DATASET_RAW = os.environ["AIRFLOW_VAR_BQ_DATASET_RAW"]
BUCKET_NAME = os.environ["AIRFLOW_VAR_GCS_BUCKET_NAME"]
SPREADSHEET_ID = os.environ["AIRFLOW_VAR_OPENAQ_SPREADSHEET_ID"]
SHEET_RANGE = os.environ["AIRFLOW_VAR_OPENAQ_SHEET_RANGE"]

# --- PATH VARIABLES ---
BASE_DIR_CSV = os.environ["AIRFLOW_VAR_LOCATIONS_BASE_PATH"]
BASE_DIR_JSON_LOCS = os.environ["AIRFLOW_VAR_LOCATIONS_JSON_PATH"]
BASE_DIR_JSON_MEAS = os.environ["AIRFLOW_VAR_MEASUREMENTS_JSON_PATH"]

# --- DATASET ---
raw_measurements_dataset = Dataset(
    f"bigquery://{PROJECT_ID}/{DATASET_RAW}/raw_measurements"
)

default_args = {
    "owner": "data_engineering",
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    dag_id="01_openaq_ingestion",
    default_args=default_args,
    start_date=datetime(2023, 1, 1),
    schedule="0 6 * * *",
    catchup=False,
    tags=["elt", "ingestion", "bronze"],
) as dag:

    # =========================================================================
    # PHASE 1: PARAMETER INGESTION
    # =========================================================================

    extract_ids_task = BashOperator(
        task_id="extract_sheets_to_gcs",
        bash_command=(
            f"python /opt/airflow/scripts/extract_sheets_to_gcs.py "
            f"{{{{ run_id }}}} {{{{ ds }}}} "
            f"{SPREADSHEET_ID} '{SHEET_RANGE}' {BUCKET_NAME} {BASE_DIR_CSV}"
        ),
    )

    load_control_table = GCSToBigQueryOperator(
        task_id="load_control_requests_to_bq",
        bucket=BUCKET_NAME,
        source_objects=[
            f"{BASE_DIR_CSV}/{{{{ execution_date.strftime('%Y/%m/%d') }}}}/locations_{{{{ run_id }}}}.csv"
        ],
        destination_project_dataset_table=f"{PROJECT_ID}.{DATASET_RAW}.control_requests",
        source_format="CSV",
        write_disposition="WRITE_APPEND",
        create_disposition="CREATE_IF_NEEDED",
        autodetect=True,
        skip_leading_rows=1,
        time_partitioning={"type": "DAY", "field": "_logical_date"},
    )

    # =========================================================================
    # PHASE 2: LOCATION DETAILS EXTRACTION
    # =========================================================================

    extract_details_task = BashOperator(
        task_id="extract_openaq_details",
        bash_command=(
            f"python /opt/airflow/scripts/extract_openaq_locations.py "
            f"{{{{ run_id }}}} {{{{ ds }}}} "
            f"{BUCKET_NAME} {BASE_DIR_CSV} {BASE_DIR_JSON_LOCS} '{{{{ conn.openaq_api.host }}}}'"
        ),
        env={"OPENAQ_API_KEY": "{{ conn.openaq_api.password }}"},
        append_env=True,
    )

    load_locations_to_bq = GCSToBigQueryOperator(
        task_id="load_locations_to_bq",
        bucket=BUCKET_NAME,
        source_objects=[
            f"{BASE_DIR_JSON_LOCS}/{{{{ execution_date.strftime('%Y/%m/%d') }}}}/locations_details_{{{{ run_id }}}}.ndjson"
        ],
        destination_project_dataset_table=f"{PROJECT_ID}.{DATASET_RAW}.raw_locations",
        source_format="NEWLINE_DELIMITED_JSON",
        write_disposition="WRITE_APPEND",
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
    # PHASE 3: MEASUREMENTS EXTRACTION
    # =========================================================================

    extract_measurements_task = BashOperator(
        task_id="extract_openaq_measurements",
        bash_command=(
            f"python /opt/airflow/scripts/extract_measurements.py "
            f"{{{{ run_id }}}} {{{{ ds }}}} "
            f"{BUCKET_NAME} {BASE_DIR_JSON_LOCS} {BASE_DIR_JSON_MEAS} '{{{{ conn.openaq_api.host }}}}'"
        ),
        env={"OPENAQ_API_KEY": "{{ conn.openaq_api.password }}"},
        append_env=True,
    )

    load_raw_measurements_task = GCSToBigQueryOperator(
        task_id="load_measurements_to_bq",
        bucket=BUCKET_NAME,
        source_objects=[
            f"{BASE_DIR_JSON_MEAS}/{{{{ execution_date.strftime('%Y/%m/%d') }}}}/measurements_{{{{ run_id }}}}*.ndjson"
        ],
        destination_project_dataset_table=f"{PROJECT_ID}.{DATASET_RAW}.raw_measurements",
        source_format="NEWLINE_DELIMITED_JSON",
        write_disposition="WRITE_APPEND",
        create_disposition="CREATE_IF_NEEDED",
        ignore_unknown_values=True,
        time_partitioning={"type": "DAY", "field": "_audit_logical_date"},
        cluster_fields=["_audit_sensor_id", "_audit_extracted_at"],
        outlets=[raw_measurements_dataset],
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

    # =========================================================================
    # ORCHESTRATION
    # =========================================================================

    extract_ids_task >> [load_control_table, extract_details_task]
    extract_details_task >> [load_locations_to_bq, extract_measurements_task]
    extract_measurements_task >> load_raw_measurements_task
