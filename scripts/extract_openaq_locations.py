"""
OpenAQ Data Extraction Pipeline

This script reads a list of location IDs from a CSV stored in Google Cloud Storage (GCS),
fetches the corresponding metadata from the OpenAQ API, appends audit fields,
and uploads the consolidated data back to GCS as an NDJSON file.
"""

import os
import argparse
import json
import logging
import requests
import time
import pandas as pd
from datetime import datetime, timezone
from google.cloud import storage
from google.auth import default

# Configure basic logging for pipeline monitoring
logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)

MAX_RETRIES = 3


def get_gcs_client():
    """
    Initializes and returns a Google Cloud Storage client using default application credentials.
    """
    creds, _ = default()
    return storage.Client(credentials=creds)


def read_input_csv(bucket_name, object_path):
    """
    Reads the Single Source of Truth (SSoT) CSV directly from GCS.

    """
    logger.info(f"Reading SSoT from: gs://{bucket_name}/{object_path}")
    return pd.read_csv(f"gs://{bucket_name}/{object_path}")


def upload_to_gcs(bucket_name, object_path, data_list):
    """
    Converts a list of dictionary records to NDJSON format and uploads it to GCS.
    """
    if not data_list:
        logger.warning("No data to upload. Skipping GCS sink operation.")
        return

    client = get_gcs_client()
    blob = client.bucket(bucket_name).blob(object_path)

    # Serialize records to Newline Delimited JSON (NDJSON)
    ndjson_content = "\n".join(
        [json.dumps(record, ensure_ascii=False) for record in data_list]
    )

    blob.upload_from_string(ndjson_content, content_type="application/x-ndjson")
    logger.info(f"File successfully saved: gs://{bucket_name}/{object_path}")


def fetch_location_data(api_url, api_key, location_id):
    """
    Fetches details for a single location ID from the OpenAQ API.
    Implements a linear backoff strategy for transient network failures.
    """
    url = f"{api_url}/locations/{location_id}"
    headers = {"X-API-Key": api_key} if api_key else {}

    for attempt in range(MAX_RETRIES):
        try:
            response = requests.get(url, headers=headers, timeout=10)

            # In case the location ID is no longer valid or missing
            if response.status_code == 404:
                return None

            response.raise_for_status()
            return response.json()

        except requests.exceptions.RequestException as e:
            logger.warning(
                f"Error fetching ID {location_id} (Attempt {attempt+1}/{MAX_RETRIES}): {e}"
            )
            # Linear backoff mechanism
            time.sleep(1 * (attempt + 1))

    return None


def main(args):
    """
    Main orchestration logic for the extraction process.
    """
    api_key = os.environ.get("OPENAQ_API_KEY", "")

    # Construct standard partition paths based on the logical date
    dt_obj = datetime.strptime(args.logical_date, "%Y-%m-%d")
    date_path = f"{dt_obj.year}/{dt_obj.month:02d}/{dt_obj.day:02d}"

    input_csv_path = f"{args.input_base_dir}/{date_path}/locations_{args.run_id}.csv"
    df = read_input_csv(args.bucket_name, input_csv_path)

    # Safely identify the ID column (defaults to "id" or falls back to the first column)
    col_id = "id" if "id" in df.columns else df.columns[0]
    location_ids = df[col_id].unique()

    logger.info(f"Processing {len(location_ids)} unique locations.")

    extracted_records = []
    output_full_path = (
        f"{args.output_base_dir}/{date_path}/locations_details_{args.run_id}.ndjson"
    )

    for loc_id in location_ids:
        raw_data = fetch_location_data(args.api_url, api_key, loc_id)

        if raw_data:
            results = raw_data.get("results", [])
            # Normalize payload structure accounting for varied API response formats (list vs dict)
            payload = (
                results[0]
                if isinstance(results, list) and results
                else (results if isinstance(results, dict) else raw_data)
            )

            # Append audit metadata for downstream lineage tracking
            extracted_records.append(
                {
                    "data": payload,
                    "_audit_run_id": args.run_id,
                    "_audit_logical_date": args.logical_date,
                    "_audit_extracted_at": datetime.now(timezone.utc).isoformat(),
                    "_audit_source": "OpenAQ API",
                    "_audit_gcs_filename": f"gs://{args.bucket_name}/{output_full_path}",
                }
            )

        # Rate limit to avoid triggering 429 Too Many Requests from the API
        time.sleep(0.1)

    upload_to_gcs(args.bucket_name, output_full_path, extracted_records)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Extract OpenAQ location details based on SSoT."
    )

    parser.add_argument(
        "run_id", help="Unique identifier for the current pipeline run."
    )
    parser.add_argument(
        "logical_date", help="Execution logical date in YYYY-MM-DD format."
    )
    parser.add_argument(
        "bucket_name", help="GCS bucket name for both input and output."
    )
    parser.add_argument("input_base_dir", help="Base directory path for the input CSV.")
    parser.add_argument(
        "output_base_dir", help="Base directory path for the output NDJSON."
    )
    parser.add_argument("api_url", help="Base URL for the OpenAQ API.")

    args = parser.parse_args()
    main(args)
