import os
import sys
from airflow import DAG, Dataset
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import (
    GCSToBigQueryOperator,
)
from airflow.utils.trigger_rule import TriggerRule
from datetime import datetime, timedelta

# Append the scripts directory to the Python path so Airflow can import your modules
sys.path.append("/opt/airflow/scripts")
from extract_sheets_to_gcs import extract_locations_data
from extract_openaq_locations import extract_locations
from extract_measurements import extract_measurements

# --- ENVIRONMENT VARIABLES ---
PROJECT_ID = os.environ["AIRFLOW_VAR_GCP_PROJECT_ID"]
DATASET_RAW = os.environ["AIRFLOW_VAR_BQ_DATASET_RAW"]
BUCKET_NAME = os.environ["AIRFLOW_VAR_GCS_BUCKET_NAME"]
SPREADSHEET_ID = os.environ["AIRFLOW_VAR_OPENAQ_SPREADSHEET_ID"]
SHEET_RANGE = os.environ["AIRFLOW_VAR_OPENAQ_SHEET_RANGE"]

# --- PATH VARIABLES ---
BASE_DIR_CSV = os.environ["AIRFLOW_VAR_LOCATIONS_BASE_PATH"]
BASE_DIR_JSON_LOCS = os.environ["AIRFLOW_VAR_LOCATIONS_JSON_PATH"]
BASE_DIR_JSON_MEAS = os.environ["AIRFLOW_VAR_MEASUREMENTS_JSON_PATH"]

# --- DATASETS ---
# Unified dataset representing the Bronze layer is fully updated and ready
bronze_ready_dataset = Dataset(f"bigquery://{PROJECT_ID}/{DATASET_RAW}/bronze_ready")

default_args = {
    "owner": "data_engineering",
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    dag_id="01_openaq_ingestion",
    max_active_runs=1, # Prevents API rate limiting and IP bans during historical backfills
    default_args=default_args,
    start_date=datetime(2025, 1, 1),
    schedule="0 6 * * *",
    catchup=True,
    tags=["elt", "ingestion", "bronze"],
) as dag:

    # =========================================================================
    # PHASE 1: PARAMETER INGESTION
    # =========================================================================

    extract_ids_task = PythonOperator(
        task_id="extract_sheets_to_gcs",
        python_callable=extract_locations_data,
        op_kwargs={
            "run_id": "{{ run_id }}",
            "logical_date_str": "{{ ds }}",
            "spreadsheet_id": SPREADSHEET_ID,
            "sheet_range": SHEET_RANGE,
            "bucket_name": BUCKET_NAME,
            "base_dir": BASE_DIR_CSV,
        },
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

    extract_details_task = PythonOperator(
        task_id="extract_openaq_details",
        python_callable=extract_locations,
        op_kwargs={
            "run_id": "{{ run_id }}",
            "logical_date": "{{ ds }}",
            "bucket_name": BUCKET_NAME,
            "input_base_dir": BASE_DIR_CSV,
            "output_base_dir": BASE_DIR_JSON_LOCS,
            "api_url": "{{ conn.openaq_api.host }}",
            "api_key": "{{ conn.openaq_api.password }}",
        },
    )

    def choose_location_branch(**kwargs):
        """
        Retrieves the processed record count from XComs.
        If records exist, proceed to BigQuery load. Otherwise, skip it.
        """
        extracted_count = kwargs["ti"].xcom_pull(task_ids="extract_openaq_details")
        if extracted_count and extracted_count > 0:
            return "load_locations_to_bq"
        return "skip_locations_load"

    branch_locations_task = BranchPythonOperator(
        task_id="branch_locations",
        python_callable=choose_location_branch,
    )

    skip_locations_load = EmptyOperator(task_id="skip_locations_load")

    load_locations_to_bq = GCSToBigQueryOperator(
        task_id="load_locations_to_bq",
        bucket=BUCKET_NAME,
        source_objects=[
            f"{BASE_DIR_JSON_LOCS}/{{{{ execution_date.strftime('%Y/%m/%d') }}}}/locations_details_{{{{ run_id }}}}_part*.ndjson"
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

    extract_measurements_task = PythonOperator(
        task_id="extract_openaq_measurements",
        python_callable=extract_measurements,
        op_kwargs={
            "run_id": "{{ run_id }}",
            "logical_date": "{{ ds }}",
            "bucket_name": BUCKET_NAME,
            "input_base_dir": BASE_DIR_JSON_LOCS,
            "output_base_dir": BASE_DIR_JSON_MEAS,
            "api_url": "{{ conn.openaq_api.host }}",
            "api_key": "{{ conn.openaq_api.password }}",
        },
    )

    def choose_measurements_branch(**kwargs):
        """
        Retrieves the processed measurements count from XComs.
        If measurements exist, proceed to BigQuery load. Otherwise, skip it.
        """
        extracted_count = kwargs["ti"].xcom_pull(task_ids="extract_openaq_measurements")
        if extracted_count and extracted_count > 0:
            return "load_measurements_to_bq"
        return "skip_measurements_load"

    branch_measurements_task = BranchPythonOperator(
        task_id="branch_measurements",
        python_callable=choose_measurements_branch,
    )

    skip_measurements_load = EmptyOperator(task_id="skip_measurements_load")

    load_raw_measurements_task = GCSToBigQueryOperator(
        task_id="load_measurements_to_bq",
        bucket=BUCKET_NAME,
        source_objects=[
            f"{BASE_DIR_JSON_MEAS}/{{{{ execution_date.strftime('%Y/%m/%d') }}}}/measurements_{{{{ run_id }}}}_part*.ndjson"
        ],
        destination_project_dataset_table=f"{PROJECT_ID}.{DATASET_RAW}.raw_measurements",
        source_format="NEWLINE_DELIMITED_JSON",
        write_disposition="WRITE_APPEND",
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

    # =========================================================================
    # PHASE 4: DATA EVALUATION & TRIGGER
    # =========================================================================

    def evaluate_bronze_updates(**kwargs):
        """
        Evaluates if ANY data was extracted during the DAG run by checking XComs.
        If either locations or measurements were updated, we emit the dataset
        to trigger downstream dbt models.
        """
        loc_count = kwargs["ti"].xcom_pull(task_ids="extract_openaq_details") or 0
        meas_count = kwargs["ti"].xcom_pull(task_ids="extract_openaq_measurements") or 0

        if loc_count > 0 or meas_count > 0:
            return "emit_dataset_trigger"
        return "skip_dataset_trigger"

    eval_data_updates = BranchPythonOperator(
        task_id="eval_data_updates",
        python_callable=evaluate_bronze_updates,
        trigger_rule=TriggerRule.NONE_FAILED_MIN_ONE_SUCCESS,
    )

    emit_dataset_trigger = EmptyOperator(
        task_id="emit_dataset_trigger",
        outlets=[bronze_ready_dataset],
    )

    skip_dataset_trigger = EmptyOperator(task_id="skip_dataset_trigger")

    # =========================================================================
    # ORCHESTRATION
    # =========================================================================

    extract_ids_task >> [load_control_table, extract_details_task]

    # Branching logic for locations
    extract_details_task >> branch_locations_task
    branch_locations_task >> [load_locations_to_bq, skip_locations_load]

    # CRITICAL CHANGE: Measurements ONLY run if locations were successfully loaded
    load_locations_to_bq >> extract_measurements_task

    # Branching logic for measurements
    extract_measurements_task >> branch_measurements_task
    branch_measurements_task >> [load_raw_measurements_task, skip_measurements_load]

    # Final evaluation. It will run successfully if ANY of these paths completes.
    # If skip_locations_load runs, the measurement tasks are automatically skipped,
    # but eval_data_updates still runs to close the DAG cleanly.
    [
        load_control_table,
        skip_locations_load,  # Direct path to the end if locations are skipped
        load_raw_measurements_task,
        skip_measurements_load,
    ] >> eval_data_updates

    eval_data_updates >> [emit_dataset_trigger, skip_dataset_trigger]
