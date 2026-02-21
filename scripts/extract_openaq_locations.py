import sys
import json
import logging
import requests
import time
import io
import pandas as pd
from datetime import datetime, timezone
from google.cloud import storage
from google.auth import default

# Logging configuration
logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)

# Retry configuration
MAX_RETRIES = 3


def get_gcs_client():
    """
    Initializes and returns a Google Cloud Storage client with default credentials.
    """
    creds, _ = default()
    return storage.Client(credentials=creds)


def read_input_csv(bucket_name, object_path):
    """
    Downloads the IDs CSV from GCS (The Single Source of Truth).
    """
    logger.info(f"Reading SSoT from: gs://{bucket_name}/{object_path}")
    client = get_gcs_client()
    bucket = client.bucket(bucket_name)
    blob = bucket.blob(object_path)

    if not blob.exists():
        raise FileNotFoundError(f"Input file not found: {object_path}")

    content = blob.download_as_text()
    return pd.read_csv(io.StringIO(content))


def upload_to_gcs(bucket_name, object_path, data_list):
    """
    Uploads the extracted data as an NDJSON file to GCS.
    """
    if not data_list:
        logger.warning("No data to upload.")
        return

    client = get_gcs_client()
    bucket = client.bucket(bucket_name)
    blob = bucket.blob(object_path)

    # Convert list of dicts to NDJSON format
    ndjson_content = "\n".join(
        [json.dumps(record, ensure_ascii=False) for record in data_list]
    )

    blob.upload_from_string(ndjson_content, content_type="application/x-ndjson")
    logger.info(f"File successfully saved: gs://{bucket_name}/{object_path}")


def fetch_location_data(api_url, api_key, location_id):
    """
    Extraction logic featuring error handling and exponential backoff for API limits.
    """
    url = f"{api_url}/locations/{location_id}"
    headers = {"X-API-Key": api_key} if api_key else {}

    for attempt in range(MAX_RETRIES):
        try:
            response = requests.get(url, headers=headers, timeout=10)

            if response.status_code == 404:
                logger.warning(f"Location ID {location_id} not found (404).")
                return None

            response.raise_for_status()
            return response.json()

        except requests.exceptions.RequestException as e:
            logger.warning(
                f"Error fetching ID {location_id} (Attempt {attempt+1}/{MAX_RETRIES}): {e}"
            )
            time.sleep(1 * (attempt + 1))  # Backoff

    logger.error(f"Permanent failure for ID {location_id} after retries.")
    return None


def main(
    run_id, logical_date, bucket_name, input_base_dir, output_base_dir, api_url, api_key
):
    """
    Main orchestration function: reads source IDs, fetches location data,
    normalizes the payload, appends audit traceability, and saves to GCS.
    """
    # 1. Reconstruct input CSV file path (SSoT)
    dt_obj = datetime.strptime(logical_date, "%Y-%m-%d")
    date_path = f"{dt_obj.year}/{dt_obj.month:02d}/{dt_obj.day:02d}"

    input_csv_path = f"{input_base_dir}/{date_path}/locations_{run_id}.csv"

    # 2. Read IDs
    df = read_input_csv(bucket_name, input_csv_path)

    # Detect ID column
    col_id = "id" if "id" in df.columns else df.columns[0]
    location_ids = df[col_id].unique()

    logger.info(f"Processing {len(location_ids)} locations.")

    extracted_records = []

    # Pre-calculate output path to inject into audit trail
    output_filename = f"locations_details_{run_id}.ndjson"
    output_full_path = f"{output_base_dir}/{date_path}/{output_filename}"
    gcs_uri = f"gs://{bucket_name}/{output_full_path}"

    # 3. Iterate and Extract
    for loc_id in location_ids:
        raw_data = fetch_location_data(api_url, api_key, loc_id)

        if raw_data:
            # Response normalization
            results = raw_data.get("results", [])
            if isinstance(results, list) and results:
                payload = results[0]
            elif isinstance(results, dict):
                payload = results
            else:
                payload = raw_data

            # 4. Enrich (Robust Auditing)
            record = {
                "data": payload,
                "_audit_run_id": run_id,
                "_audit_logical_date": logical_date,
                "_audit_extracted_at": datetime.now(timezone.utc).isoformat(),
                "_audit_source": "OpenAQ API",
                "_audit_gcs_filename": gcs_uri,
            }
            extracted_records.append(record)

        time.sleep(0.1)  # Rate limit

    # 5. Save
    upload_to_gcs(bucket_name, output_full_path, extracted_records)


if __name__ == "__main__":
    if len(sys.argv) < 8:
        logger.error("Incorrect argument usage. Exiting.")
        sys.exit(1)

    main(
        run_id=sys.argv[1],
        logical_date=sys.argv[2],
        bucket_name=sys.argv[3],
        input_base_dir=sys.argv[4],
        output_base_dir=sys.argv[5],
        api_url=sys.argv[6],
        api_key=sys.argv[7],
    )
