"""
Google Sheets Data Extraction Pipeline

This script extracts data from a specified Google Sheet, enriches it with
audit metadata (run ID, logical date, ingestion timestamp), and uploads
it directly to Google Cloud Storage (GCS) as a CSV file using standard libraries.
"""

import io
import csv
import logging
from datetime import datetime, timezone
from google.auth import default
import googleapiclient.discovery

# Import common utilities
from gcs_utils import upload_string_to_gcs, get_partition_path

# Configure basic logging for pipeline monitoring
logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)


def extract_locations_data(
    run_id, logical_date_str, spreadsheet_id, sheet_range, bucket_name, base_dir
):
    """
    Callable orchestration logic designed for Airflow's PythonOperator.
    Extracts data from Google Sheets, enriches it, and uploads it to GCS.
    """
    logger.info(f"Starting Sheets extraction for Run ID: {run_id}")

    # 1. Authentication & Sheets API Setup
    scopes = ["https://www.googleapis.com/auth/spreadsheets.readonly"]
    creds, _ = default(scopes=scopes)
    service = googleapiclient.discovery.build("sheets", "v4", credentials=creds)

    # 2. Extract Data from Google Sheets
    sheet = service.spreadsheets()
    result = (
        sheet.values().get(spreadsheetId=spreadsheet_id, range=sheet_range).execute()
    )
    values = result.get("values", [])

    if not values:
        raise ValueError(
            f"No data found in the specified Google Sheet range: {sheet_range}"
        )

    # 3. Path Construction
    file_name = f"locations_{run_id}.csv"
    date_path = get_partition_path(logical_date_str)
    full_object_path = f"{base_dir}/{date_path}/{file_name}"
    source_file_path = f"gs://{bucket_name}/{full_object_path}"
    ingestion_ts = datetime.now(timezone.utc).isoformat()

    # 4. Data Enrichment (Audit Columns without Pandas)
    headers = values[0]
    original_header_len = len(headers)

    audit_headers = [
        "_ingestion_timestamp",
        "_logical_date",
        "_airflow_run_id",
        "_source_file",
    ]
    headers.extend(audit_headers)

    enriched_data = [headers]

    for row in values[1:]:
        # Google Sheets sometimes drops trailing empty cells in a row; pad them if necessary
        while len(row) < original_header_len:
            row.append("")

        audit_values = [ingestion_ts, logical_date_str, run_id, source_file_path]
        row.extend(audit_values)
        enriched_data.append(row)

    # 5. Prepare CSV Content (In-memory string conversion via standard library)
    csv_buffer = io.StringIO()
    csv_writer = csv.writer(csv_buffer)
    csv_writer.writerows(enriched_data)
    csv_content = csv_buffer.getvalue()

    # 6. Upload
    upload_string_to_gcs(
        bucket_name=bucket_name,
        object_path=full_object_path,
        content=csv_content,
        content_type="text/csv",
    )

    logger.info("Sheets data successfully enriched and uploaded.")
