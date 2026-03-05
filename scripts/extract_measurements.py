"""
OpenAQ Measurements Extraction Pipeline

This script parses location metadata from GCS to extract active sensor IDs,
fetches historical measurements (time-series data) for those sensors from the OpenAQ API
using persistent HTTP sessions, and streams the results back to GCS in chunked NDJSON files.
"""

import json
import logging
import requests
import time
from datetime import datetime, timezone, timedelta

# Import common utilities (Fallback logic removed as Airflow branching handles it now)
from gcs_utils import (
    get_gcs_client,
    cleanup_previous_run_files,
    upload_chunk_to_gcs,
)

logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)


def get_sensor_ids_from_locations_json(bucket_name, input_prefix, run_id):
    """
    Reads upstream location data from GCS (SSoT) to extract a unique set of sensor IDs.
    Accounts for chunked file outputs (e.g., _part0.ndjson).
    """
    client = get_gcs_client()
    bucket = client.bucket(bucket_name)
    blobs = list(bucket.list_blobs(prefix=input_prefix))

    # Target the base name to catch all chunks (part0, part1, etc.)
    target_base = f"locations_details_{run_id}"
    sensor_ids = set()

    for blob in blobs:
        # Extract just the filename from the full GCS path
        filename = blob.name.split("/")[-1]

        # Check if the file belongs to the current run
        if not filename.startswith(target_base):
            continue

        with blob.open("rt") as f:
            for line in f:
                if not line.strip():
                    continue
                try:
                    record = json.loads(line)
                    sensors = record.get("data", {}).get("sensors", [])
                    for s in sensors:
                        if "id" in s:
                            sensor_ids.add(s["id"])
                except json.JSONDecodeError:
                    continue

    logger.info(f"Unique sensor IDs to process: {len(sensor_ids)}")
    return list(sensor_ids)


def fetch_measurements(session, api_url, sensor_id, date_from, date_to):
    """
    Retrieves time-series measurements for a specific sensor within a time window.
    Implements robust pagination with exponential backoff for transient errors,
    fails loudly to prevent silent data loss, and safely parses the JSON payload.
    """
    url = f"{api_url}/sensors/{sensor_id}/measurements"
    params = {
        "limit": 1000,
        "page": 1,
        "datetime_from": date_from,
        "datetime_to": date_to,
    }

    all_measurements = []
    seen_measurement_ids = set()
    max_retries = 3

    while True:
        page_success = False

        # 1. Micro-retries for individual page failures
        for attempt in range(max_retries):
            try:
                response = session.get(url, params=params, timeout=15)

                if response.status_code == 404:
                    return (
                        all_measurements  # 404 naturally means no data for this sensor
                    )

                response.raise_for_status()

                results = response.json().get("results", [])

                for res in results:
                    # Guard clause: ensure 'res' is actually a dictionary
                    if not isinstance(res, dict):
                        logger.warning(
                            f"Unexpected record format received, skipping: {res}"
                        )
                        continue

                    # Safe traversal: use 'or {}' to prevent AttributeError if an intermediate key is None
                    period = res.get("period") or {}
                    dt_from = period.get("datetimeFrom") or {}
                    utc_time = dt_from.get("utc")
                    value = res.get("value")

                    unique_key = f"{utc_time}-{value}"
                    if unique_key not in seen_measurement_ids:
                        seen_measurement_ids.add(unique_key)
                        all_measurements.append(res)

                page_success = True
                break  # Page fetched successfully, break the retry loop

            except requests.exceptions.RequestException as e:
                logger.warning(
                    f"Network error on sensor {sensor_id}, page {params['page']} "
                    f"(Attempt {attempt + 1}/{max_retries}): {e}"
                )
                time.sleep(2**attempt)  # Exponential backoff: 1s, 2s, 4s

        # 2. Fail Loudly: If all retries for this page failed, CRASH the script
        if not page_success:
            logger.error(
                f"FATAL: Exhausted retries for sensor {sensor_id}, page {params['page']}."
            )
            raise RuntimeError(
                f"Incomplete data extraction for sensor {sensor_id}. "
                f"Failing task to trigger Airflow retry and prevent silent data loss."
            )

        # If page succeeded but returned less than limit, we reached the end
        if len(results) < params["limit"]:
            break

        params["page"] += 1
        time.sleep(0.1)

    return all_measurements


def extract_measurements(
    run_id, logical_date, bucket_name, input_base_dir, output_base_dir, api_url, api_key
):
    """
    Callable orchestration logic designed for Airflow's PythonOperator.
    Returns the total number of processed records to enable downstream branching.
    """
    dt_run = datetime.strptime(logical_date, "%Y-%m-%d").replace(tzinfo=timezone.utc)
    date_from = dt_run.isoformat()
    date_to = (dt_run + timedelta(days=1)).isoformat()
    path_date = f"{dt_run.year}/{dt_run.month:02d}/{dt_run.day:02d}"
    batch_ts = datetime.now(timezone.utc).isoformat()

    run_output_prefix = f"{output_base_dir}/{path_date}/measurements_{run_id}"

    # Enforce idempotency
    cleanup_previous_run_files(bucket_name, run_output_prefix)

    sensor_ids = get_sensor_ids_from_locations_json(
        bucket_name, f"{input_base_dir}/{path_date}/", run_id
    )

    # Return 0 immediately if no sensors to process (triggers skip in Airflow)
    if not sensor_ids:
        logger.warning(
            "No sensors found in the locations file. Returning 0 to skip downstream loads."
        )
        return 0

    buffer, chunk_size, file_counter, total_processed = [], 2000, 0, 0

    # Initialize a persistent HTTP session to avoid TCP/SSL handshake overhead per request
    with requests.Session() as session:
        if api_key:
            session.headers.update({"X-API-Key": api_key})

        for sensor_id in sensor_ids:
            measurements = fetch_measurements(
                session, api_url, sensor_id, date_from, date_to
            )

            for m in measurements:
                buffer.append(
                    {
                        "data": m,
                        "_audit_run_id": run_id,
                        "_audit_sensor_id": sensor_id,
                        "_audit_logical_date": logical_date,
                        "_audit_extracted_at": batch_ts,
                    }
                )

            total_processed += len(measurements)

            # Check if buffer reached chunk size to offload memory
            if len(buffer) >= chunk_size:
                output_path = f"{run_output_prefix}_part{file_counter}.ndjson"
                for r in buffer:
                    r["_audit_gcs_filename"] = f"gs://{bucket_name}/{output_path}"
                upload_chunk_to_gcs(bucket_name, output_path, buffer)
                buffer, file_counter = [], file_counter + 1

            time.sleep(0.1)

    # Upload any remaining records in the buffer
    if buffer:
        output_path = f"{run_output_prefix}_part{file_counter}.ndjson"
        for r in buffer:
            r["_audit_gcs_filename"] = f"gs://{bucket_name}/{output_path}"
        upload_chunk_to_gcs(bucket_name, output_path, buffer)

    logger.info(f"Extraction complete. Total measurements processed: {total_processed}")

    # Return the row count so Airflow XCom can capture it for BranchPythonOperator
    return total_processed
