"""
OpenAQ Measurements Extraction Pipeline

This script parses location metadata from GCS to extract active sensor IDs,
fetches historical measurements (time-series data) for those sensors from the OpenAQ API,
and streams the results back to GCS in chunked NDJSON files.
"""

import os
import argparse
import json
import logging
import requests
import time
from datetime import datetime, timezone, timedelta
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
    Initializes and returns a Google Cloud Storage client.
    """
    creds, _ = default()
    return storage.Client(credentials=creds)


def get_sensor_ids_from_locations_json(bucket_name, input_prefix, run_id):
    """
    Reads upstream location data from GCS (SSoT) to extract a unique set of sensor IDs.
    Implements a streaming read approach to guarantee memory safety.
    """
    client = get_gcs_client()
    bucket = client.bucket(bucket_name)

    blobs = list(bucket.list_blobs(prefix=input_prefix))

    # Filter by pipeline run ID to prevent mixing data from previous or parallel executions (idempotency constraint)
    target_filename = f"locations_details_{run_id}.ndjson"
    sensor_ids = set()

    for blob in blobs:
        if not blob.name.endswith(target_filename):
            continue

        # Process the NDJSON file stream line-by-line.
        # This prevents Out-Of-Memory (OOM) errors when handling gigabyte-scale files.
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
                    # Silently skip malformed JSON lines to keep the pipeline running
                    continue

    logger.info(f"Unique sensor IDs to process: {len(sensor_ids)}")
    return list(sensor_ids)


def upload_chunk_to_gcs(bucket_name, object_path, data_list):
    """
    Serializes a chunk of data to NDJSON and uploads it to the GCS sink.
    """
    if not data_list:
        return

    client = get_gcs_client()
    blob = client.bucket(bucket_name).blob(object_path)

    ndjson_content = "\n".join(
        [json.dumps(record, ensure_ascii=False) for record in data_list]
    )
    blob.upload_from_string(ndjson_content, content_type="application/x-ndjson")
    logger.info(
        f"Chunk saved: gs://{bucket_name}/{object_path} ({len(data_list)} entries)"
    )


def fetch_measurements(api_url, api_key, sensor_id, date_from, date_to):
    """
    Retrieves time-series measurements for a specific sensor within a time window.
    Handles API pagination and deduplicates overlapping records.
    """
    url = f"{api_url}/sensors/{sensor_id}/measurements"
    headers = {"X-API-Key": api_key} if api_key else {}
    params = {
        "limit": 1000,
        "page": 1,
        "datetime_from": date_from,
        "datetime_to": date_to,
    }

    all_measurements = []
    seen_measurement_ids = set()

    while True:
        try:
            response = requests.get(url, headers=headers, params=params, timeout=15)

            if response.status_code == 404:
                return []

            response.raise_for_status()

            results = response.json().get("results", [])
            for res in results:
                try:
                    # Construct a composite key to deduplicate records that might overlap across pages
                    unique_key = f"{res.get('period', {}).get('datetimeFrom', {}).get('utc')}-{res.get('value')}"
                    if unique_key not in seen_measurement_ids:
                        seen_measurement_ids.add(unique_key)
                        all_measurements.append(res)
                except Exception:
                    # Fallback: if the composite key fails due to schema changes, append anyway
                    all_measurements.append(res)

            # Break pagination loop if the returned batch is smaller than the limit
            if len(results) < params["limit"]:
                break

            params["page"] += 1
            time.sleep(0.1)

        except Exception as e:
            logger.error(f"Error extracting sensor {sensor_id}: {e}")
            break

    return all_measurements


def main(args):
    api_key = os.environ.get("OPENAQ_API_KEY", "")

    dt_run = datetime.strptime(args.logical_date, "%Y-%m-%d").replace(
        tzinfo=timezone.utc
    )
    date_to = dt_run.isoformat()
    date_from = (dt_run - timedelta(days=1)).isoformat()
    path_date = f"{dt_run.year}/{dt_run.month:02d}/{dt_run.day:02d}"
    batch_ts = datetime.now(timezone.utc).isoformat()

    sensor_ids = get_sensor_ids_from_locations_json(
        args.bucket_name, f"{args.input_base_dir}/{path_date}/", args.run_id
    )

    if not sensor_ids:
        logger.warning("No sensors found.")
        # Ensure empty file is created even if no sensors exist
        empty_path = f"{args.output_base_dir}/{path_date}/measurements_{args.run_id}_EMPTY.ndjson"
        get_gcs_client().bucket(args.bucket_name).blob(empty_path).upload_from_string(
            "", content_type="application/x-ndjson"
        )
        return

    buffer, chunk_size, file_counter, total_processed = [], 2000, 0, 0

    for sensor_id in sensor_ids:
        measurements = fetch_measurements(
            args.api_url, api_key, sensor_id, date_from, date_to
        )

        for m in measurements:
            buffer.append(
                {
                    "data": m,
                    "_audit_run_id": args.run_id,
                    "_audit_sensor_id": sensor_id,
                    "_audit_logical_date": args.logical_date,
                    "_audit_extracted_at": batch_ts,
                }
            )

        total_processed += len(measurements)

        if len(buffer) >= chunk_size:
            output_path = f"{args.output_base_dir}/{path_date}/measurements_{args.run_id}_part{file_counter}.ndjson"
            for r in buffer:
                r["_audit_gcs_filename"] = f"gs://{args.bucket_name}/{output_path}"
            upload_chunk_to_gcs(args.bucket_name, output_path, buffer)
            buffer, file_counter = [], file_counter + 1

        time.sleep(0.05)

    if buffer:
        output_path = f"{args.output_base_dir}/{path_date}/measurements_{args.run_id}_part{file_counter}.ndjson"
        for r in buffer:
            r["_audit_gcs_filename"] = f"gs://{args.bucket_name}/{output_path}"
        upload_chunk_to_gcs(args.bucket_name, output_path, buffer)

    # STRICT FALLBACK: Always generate a file for BigQuery to consume
    if total_processed == 0:
        logger.warning(
            "No measurements found. Generating empty file to satisfy downstream dependencies."
        )
        empty_path = f"{args.output_base_dir}/{path_date}/measurements_{args.run_id}_EMPTY.ndjson"
        get_gcs_client().bucket(args.bucket_name).blob(empty_path).upload_from_string(
            "", content_type="application/x-ndjson"
        )


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Extract OpenAQ sensor measurements based on upstream locations."
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
    parser.add_argument(
        "input_base_dir", help="Base directory path for the input NDJSON."
    )
    parser.add_argument(
        "output_base_dir", help="Base directory path for the output measurements."
    )
    parser.add_argument("api_url", help="Base URL for the OpenAQ API.")

    args = parser.parse_args()
    main(args)
