"""
Google Sheets Data Extraction Pipeline

This script extracts data from a specified Google Sheet, enriches it with
audit metadata (run ID, logical date, ingestion timestamp), and uploads
it directly to Google Cloud Storage (GCS) as a CSV file.
"""

import sys
import argparse
import logging
import pandas as pd
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
    Main orchestration logic to extract data from Google Sheets,
    enrich it with audit metadata, and upload it to GCS as a CSV file.
    """
    try:
        logger.info(f"Starting Sheets extraction for Run ID: {run_id}")

        # 1. Authentication & Sheets API Setup
        # Only request Sheets read-only scope. GCS auth is handled centrally by gcs_utils.
        scopes = ["https://www.googleapis.com/auth/spreadsheets.readonly"]
        creds, _ = default(scopes=scopes)

        service = googleapiclient.discovery.build("sheets", "v4", credentials=creds)

        # 2. Extract Data from Google Sheets
        sheet = service.spreadsheets()
        result = (
            sheet.values()
            .get(spreadsheetId=spreadsheet_id, range=sheet_range)
            .execute()
        )
        values = result.get("values", [])

        if not values:
            raise ValueError("No data found in the specified Google Sheet range.")

        # Create DataFrame (Assuming the first row contains headers)
        df = pd.DataFrame(values[1:], columns=values[0])

        # 3. Path Construction
        file_name = f"locations_{run_id}.csv"
        date_path = get_partition_path(logical_date_str)
        full_object_path = f"{base_dir}/{date_path}/{file_name}"

        # 4. Data Enrichment (Audit Columns for data lineage)
        df["_ingestion_timestamp"] = datetime.now(timezone.utc).isoformat()
        df["_logical_date"] = logical_date_str
        df["_airflow_run_id"] = run_id
        df["_source_file"] = f"gs://{bucket_name}/{full_object_path}"

        # 5. Prepare CSV Content (In-memory string conversion)
        csv_content = df.to_csv(index=False)

        # 6. Upload
        upload_string_to_gcs(
            bucket_name=bucket_name,
            object_path=full_object_path,
            content=csv_content,
            content_type="text/csv",
        )

    except Exception as e:
        logger.error(f"Critical error in extraction process: {e}")
        sys.exit(1)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Extract data from a Google Spreadsheet and upload it to GCS as a CSV."
    )

    parser.add_argument(
        "run_id",
        help="Unique identifier for the current pipeline run (e.g., from Airflow).",
    )
    parser.add_argument(
        "logical_date", help="Execution logical date in YYYY-MM-DD format."
    )
    parser.add_argument(
        "spreadsheet_id", help="The unique ID of the Google Spreadsheet to read from."
    )
    parser.add_argument(
        "sheet_range",
        help="The A1 notation of the range to extract (e.g., 'Locations!A1:Z').",
    )
    parser.add_argument(
        "bucket_name", help="GCS bucket name where the output CSV will be stored."
    )
    parser.add_argument("base_dir", help="Base directory path within the GCS bucket.")

    args = parser.parse_args()

    # Execute the extraction process with parsed arguments
    extract_locations_data(
        run_id=args.run_id,
        logical_date_str=args.logical_date,
        spreadsheet_id=args.spreadsheet_id,
        sheet_range=args.sheet_range,
        bucket_name=args.bucket_name,
        base_dir=args.base_dir,
    )
