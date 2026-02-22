from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import (
    GCSToBigQueryOperator,
)
from airflow.models import Variable
from airflow.utils.dates import days_ago
from datetime import timedelta

# --- GLOBAL CONFIGURATION ---
# Using Airflow Variables with defaults ensures code portability across environments (Dev/Prod)
SPREADSHEET_ID = Variable.get("OPENAQ_SPREADSHEET_ID")
SHEET_RANGE = Variable.get("OPENAQ_SHEET_RANGE")
BUCKET_NAME = Variable.get("GCS_BUCKET_NAME")
PROJECT_ID = Variable.get("GCP_PROJECT_ID")
DATASET_RAW = Variable.get("BQ_DATASET_RAW")

# --- GCS PATHS (Namespaces) ---
BASE_DIR_CSV = Variable.get("LOCATIONS_BASE_PATH", default_var="raw/locations_id_csv")
BASE_DIR_JSON_LOCS = Variable.get(
    "LOCATIONS_JSON_PATH", default_var="raw/locations_details_json"
)
BASE_DIR_JSON_MEAS = Variable.get(
    "MEASUREMENTS_JSON_PATH", default_var="raw/measurements_json"
)

# --- DEFAULT ARGS ---
default_args = {
    "owner": "data_engineering",
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
    "email_on_failure": False,
}

with DAG(
    dag_id="01_ingest_locations_sheets",
    default_args=default_args,
    start_date=days_ago(1),
    schedule="0 6 * * *",  # 6:00 AM UTC
    catchup=False,
    tags=["raw", "google_sheets", "openaq", "elt", "production"],
) as dag:

    # Date Variables for Paths
    year = "{{ execution_date.year }}"
    month = "{{ execution_date.strftime('%m') }}"
    day = "{{ execution_date.strftime('%d') }}"

    # Secure credential extraction using Airflow macros (Prevents top-level DB calls)
    API_URL = "{{ conn.openaq_api.host | default('https://api.openaq.org/v3') }}"
    API_KEY = "{{ conn.openaq_api.password | default('') }}"

    # =========================================================================
    # PHASE 1: PARAMETER INGESTION (SSoT)
    # =========================================================================

    extract_ids_task = BashOperator(
        task_id="extract_sheets_to_gcs",
        bash_command=(
            "python /opt/airflow/scripts/extract_sheets_to_gcs.py "
            "{{ run_id }} {{ ds }} "
            f"{SPREADSHEET_ID} '{SHEET_RANGE}' {BUCKET_NAME} {BASE_DIR_CSV}"
        ),
    )

    load_control_table = GCSToBigQueryOperator(
        task_id="load_control_requests_to_bq",
        bucket=BUCKET_NAME,
        source_objects=[
            f"{BASE_DIR_CSV}/{year}/{month}/{day}/locations_{{{{ run_id }}}}.csv"
        ],
        destination_project_dataset_table=f"{PROJECT_ID}.{DATASET_RAW}.control_requests",
        source_format="CSV",
        write_disposition="WRITE_APPEND",
        create_disposition="CREATE_IF_NEEDED",
        autodetect=True,
        skip_leading_rows=1,
        allow_quoted_newlines=True,
        time_partitioning={"type": "DAY", "field": "_logical_date"},
    )

    # =========================================================================
    # PHASE 2: LOCATION DETAILS EXTRACTION (Dimensions)
    # =========================================================================

    extract_details_task = BashOperator(
        task_id="extract_openaq_details",
        bash_command=(
            "python /opt/airflow/scripts/extract_openaq_locations.py "
            "{{ run_id }} {{ ds }} "
            f"{BUCKET_NAME} {BASE_DIR_CSV} {BASE_DIR_JSON_LOCS} '{API_URL}' '{API_KEY}'"
        ),
    )

    load_raw_locations_task = GCSToBigQueryOperator(
        task_id="load_locations_to_bq",
        bucket=BUCKET_NAME,
        source_objects=[
            f"{BASE_DIR_JSON_LOCS}/{year}/{month}/{day}/locations_details_{{{{ run_id }}}}.ndjson"
        ],
        destination_project_dataset_table=f"{PROJECT_ID}.{DATASET_RAW}.raw_locations",
        source_format="NEWLINE_DELIMITED_JSON",
        write_disposition="WRITE_APPEND",
        create_disposition="CREATE_IF_NEEDED",
        ignore_unknown_values=True,
        schema_fields=[
            {"name": "data", "type": "JSON", "mode": "NULLABLE"},
            {"name": "_audit_run_id", "type": "STRING", "mode": "NULLABLE"},
            {"name": "_audit_logical_date", "type": "DATE", "mode": "NULLABLE"},
            {"name": "_audit_extracted_at", "type": "TIMESTAMP", "mode": "NULLABLE"},
            {"name": "_audit_source", "type": "STRING", "mode": "NULLABLE"},
            {"name": "_audit_gcs_filename", "type": "STRING", "mode": "NULLABLE"},
        ],
        time_partitioning={"type": "DAY", "field": "_audit_logical_date"},
    )

    # =========================================================================
    # PHASE 3: MEASUREMENTS EXTRACTION (Facts)
    # =========================================================================

    extract_measurements_task = BashOperator(
        task_id="extract_openaq_measurements",
        bash_command=(
            "python /opt/airflow/scripts/extract_measurements.py "
            "{{ run_id }} {{ ds }} "
            f"{BUCKET_NAME} {BASE_DIR_JSON_LOCS} {BASE_DIR_JSON_MEAS} '{API_URL}' '{API_KEY}'"
        ),
    )

    load_raw_measurements_task = GCSToBigQueryOperator(
        task_id="load_measurements_to_bq",
        bucket=BUCKET_NAME,
        source_objects=[
            f"{BASE_DIR_JSON_MEAS}/{year}/{month}/{day}/measurements_{{{{ run_id }}}}*.ndjson"
        ],
        destination_project_dataset_table=f"{PROJECT_ID}.{DATASET_RAW}.raw_measurements",
        source_format="NEWLINE_DELIMITED_JSON",
        write_disposition="WRITE_APPEND",
        create_disposition="CREATE_IF_NEEDED",
        ignore_unknown_values=True,
        schema_fields=[
            {"name": "data", "type": "JSON", "mode": "NULLABLE"},
            {"name": "_audit_run_id", "type": "STRING", "mode": "NULLABLE"},
            {"name": "_audit_sensor_id", "type": "INTEGER", "mode": "NULLABLE"},
            {"name": "_audit_logical_date", "type": "DATE", "mode": "NULLABLE"},
            {"name": "_audit_extracted_at", "type": "TIMESTAMP", "mode": "NULLABLE"},
            {"name": "_audit_gcs_filename", "type": "STRING", "mode": "NULLABLE"},
        ],
        time_partitioning={"type": "DAY", "field": "_audit_logical_date"},
        cluster_fields=["_audit_sensor_id", "_audit_extracted_at"],
    )

    # =========================================================================
    # PHASE 4: DATA TRANSFORMATION (dbt)
    # =========================================================================

    run_dbt_models_task = BashOperator(
        task_id="run_dbt_models",
        bash_command=(
            "source /opt/airflow/dbt_venv/bin/activate && "
            "cd /opt/airflow/openaq_transform && "
            "dbt deps && "
            "dbt build --profiles-dir ."
        ),
    )

    # =========================================================================
    # ORCHESTRATION
    # =========================================================================

    extract_ids_task >> [load_control_table, extract_details_task]
    extract_details_task >> [load_raw_locations_task, extract_measurements_task]
    extract_measurements_task >> load_raw_measurements_task
    load_raw_measurements_task >> run_dbt_models_task
