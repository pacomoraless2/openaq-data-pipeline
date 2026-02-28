"""
OpenAQ Data Extraction Pipeline

This script reads a list of location IDs from a GCS CSV,
fetches the corresponding metadata from the OpenAQ API using persistent sessions,
and uploads the consolidated data back to GCS as chunked NDJSON files.
"""

import os
import json
import logging
import requests
import time
import pandas as pd
from datetime import datetime, timezone

# Import common utilities
from gcs_utils import (
    cleanup_previous_run_files,
    upload_chunk_to_gcs,
)

logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)

MAX_RETRIES = 3


def read_input_csv(bucket_name, object_path):
    """
    Reads the Single Source of Truth (SSoT) CSV directly from GCS.
    """
    logger.info(f"Reading SSoT from: gs://{bucket_name}/{object_path}")
    return pd.read_csv(f"gs://{bucket_name}/{object_path}")


def fetch_location_data(session, api_url, location_id):
    """
    Fetches details for a single location ID using a persistent HTTP session.
    Implements a linear backoff strategy for transient network failures.
    """
    url = f"{api_url}/locations/{location_id}"

    for attempt in range(MAX_RETRIES):
        try:
            # Reusing the TCP connection via session.get instead of requests.get
            response = session.get(url, timeout=10)
            if response.status_code == 404:
                return None
            response.raise_for_status()
            return response.json()
        except requests.exceptions.RequestException as e:
            logger.warning(
                f"Error fetching ID {location_id} (Attempt {attempt+1}/{MAX_RETRIES}): {e}"
            )
            time.sleep(1 * (attempt + 1))

    return None


def extract_locations(
    run_id, logical_date, bucket_name, input_base_dir, output_base_dir, api_url, api_key
):
    """
    Callable orchestration logic designed for Airflow's PythonOperator.
    Returns the total number of processed records to enable downstream branching.
    """
    dt_obj = datetime.strptime(logical_date, "%Y-%m-%d")
    date_path = f"{dt_obj.year}/{dt_obj.month:02d}/{dt_obj.day:02d}"

    input_csv_path = f"{input_base_dir}/{date_path}/locations_{run_id}.csv"
    run_output_prefix = f"{output_base_dir}/{date_path}/locations_details_{run_id}"

    # Use imported utility for idempotency
    cleanup_previous_run_files(bucket_name, run_output_prefix)

    df = read_input_csv(bucket_name, input_csv_path)
    col_id = "id" if "id" in df.columns else df.columns[0]
    location_ids = df[col_id].unique()

    logger.info(f"Processing {len(location_ids)} unique locations.")

    # Return 0 immediately if no locations to process (triggers skip in Airflow)
    if len(location_ids) == 0:
        logger.warning("No locations found. Returning 0 to skip downstream loads.")
        return 0

    buffer, chunk_size, file_counter, total_processed = [], 1000, 0, 0
    batch_ts = datetime.now(timezone.utc).isoformat()

    # Initialize a persistent HTTP session to avoid TCP/SSL handshake overhead per request
    with requests.Session() as session:
        if api_key:
            session.headers.update({"X-API-Key": api_key})

        for loc_id in location_ids:
            raw_data = fetch_location_data(session, api_url, loc_id)

            if raw_data:
                results = raw_data.get("results", [])
                payload = (
                    results[0]
                    if isinstance(results, list) and results
                    else (results if isinstance(results, dict) else raw_data)
                )

                output_full_path = f"{run_output_prefix}_part{file_counter}.ndjson"

                buffer.append(
                    {
                        "data": payload,
                        "_audit_run_id": run_id,
                        "_audit_logical_date": logical_date,
                        "_audit_extracted_at": batch_ts,
                        "_audit_source": "OpenAQ API",
                        "_audit_gcs_filename": f"gs://{bucket_name}/{output_full_path}",
                    }
                )
                total_processed += 1

            if len(buffer) >= chunk_size:
                upload_chunk_to_gcs(bucket_name, output_full_path, buffer)
                buffer, file_counter = [], file_counter + 1

            time.sleep(0.1)

    if buffer:
        output_full_path = f"{run_output_prefix}_part{file_counter}.ndjson"
        for r in buffer:
            r["_audit_gcs_filename"] = f"gs://{bucket_name}/{output_full_path}"
        upload_chunk_to_gcs(bucket_name, output_full_path, buffer)

    logger.info(
        f"Extraction complete. Total valid locations processed: {total_processed}"
    )

    # Return the row count so Airflow XCom can capture it for BranchPythonOperator
    return total_processed
