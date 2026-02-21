import sys
import logging
import pandas as pd
from datetime import datetime, timezone
from google.cloud import storage
from google.auth import default
import googleapiclient.discovery

# Logging Configuration
logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)


def upload_to_gcs_generic(bucket_name, object_path, content, content_type="text/plain"):
    """
    Generic function to upload content to Google Cloud Storage.
    Designed to be reusable for CSV, JSON, or any other format.
    """
    try:
        # Get default credentials from the environment (Airflow/Compute Engine)
        creds, _ = default()
        storage_client = storage.Client(credentials=creds)
        bucket = storage_client.bucket(bucket_name)
        blob = bucket.blob(object_path)

        blob.upload_from_string(content, content_type=content_type)
        logger.info(f"Successfully uploaded to: gs://{bucket_name}/{object_path}")

    except Exception as e:
        logger.error(f"Failed to upload to GCS: {e}")
        raise


def extract_locations_data(
    run_id, logical_date_str, spreadsheet_id, sheet_range, bucket_name, base_dir
):
    """
    Main orchestration logic to extract data from Google Sheets,
    enrich it with audit metadata, and upload it to GCS as a CSV file.
    """
    try:
        logger.info(f"Starting extraction for Run ID: {run_id}")

        # 1. Authentication & Sheets API Setup
        scopes = [
            "https://www.googleapis.com/auth/spreadsheets.readonly",
            "https://www.googleapis.com/auth/devstorage.read_write",
        ]
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
            raise ValueError("No data found in Google Sheet.")

        # Create DataFrame (Assuming first row is header)
        df = pd.DataFrame(values[1:], columns=values[0])

        # 3. Data Enrichment (Audit Columns)
        # Parse logical date for path construction
        dt_obj = datetime.strptime(logical_date_str, "%Y-%m-%d")

        df["_ingestion_timestamp"] = datetime.now(timezone.utc).isoformat()
        df["_logical_date"] = logical_date_str
        df["_airflow_run_id"] = run_id

        # 4. Path Construction
        # Structure: base_dir/YYYY/MM/DD/filename.csv
        file_name = f"locations_{run_id}.csv"
        date_path = f"{dt_obj.year}/{dt_obj.month:02d}/{dt_obj.day:02d}"
        full_object_path = f"{base_dir}/{date_path}/{file_name}"

        # Add generic lineage metadata to the row itself
        df["_source_file"] = f"gs://{bucket_name}/{full_object_path}"

        # 5. Prepare CSV Content (In-memory)
        csv_content = df.to_csv(index=False)

        # 6. Upload using the generic function
        upload_to_gcs_generic(
            bucket_name=bucket_name,
            object_path=full_object_path,
            content=csv_content,
            content_type="text/csv",
        )

    except Exception as e:
        logger.error(f"Critical error in extraction process: {e}")
        sys.exit(1)


if __name__ == "__main__":
    # Argument validation
    if len(sys.argv) < 7:
        logger.error(
            "Usage: script.py <run_id> <logical_date> <spreadsheet_id> <range> <bucket> <base_dir>"
        )
        sys.exit(1)

    # Parsing arguments passed from Airflow
    extract_locations_data(
        run_id=sys.argv[1],
        logical_date_str=sys.argv[2],
        spreadsheet_id=sys.argv[3],
        sheet_range=sys.argv[4],
        bucket_name=sys.argv[5],
        base_dir=sys.argv[6],
    )
