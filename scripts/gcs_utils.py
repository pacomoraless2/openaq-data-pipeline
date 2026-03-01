"""
Common Utilities Module for GCS Operations

This module centralizes Google Cloud Storage interactions for the OpenAQ
data extraction pipelines, ensuring DRY principles and consistent behavior
across different extraction scripts.
"""

import json
import logging
from datetime import datetime
from google.cloud import storage
from google.auth import default

logger = logging.getLogger(__name__)


def get_gcs_client():
    """
    Initializes and returns a Google Cloud Storage client using default application credentials.
    """
    creds, _ = default()
    return storage.Client(credentials=creds)


def cleanup_previous_run_files(bucket_name, prefix_path):
    """
    Deletes any existing files from a previous (potentially failed) run
    to ensure idempotency and prevent data duplication in downstream tasks.
    """
    client = get_gcs_client()
    bucket = client.bucket(bucket_name)

    blobs_to_delete = list(bucket.list_blobs(prefix=prefix_path))

    if blobs_to_delete:
        logger.warning(
            f"Found {len(blobs_to_delete)} existing files for prefix '{prefix_path}'. Deleting for a clean slate..."
        )
        for blob in blobs_to_delete:
            blob.delete()
        logger.info("Cleanup of previous run files completed successfully.")
    else:
        logger.info("No previous run files found. Clean slate confirmed.")


def upload_chunk_to_gcs(bucket_name, object_path, data_list):
    """
    Converts a list of dictionary records to NDJSON format and uploads it to GCS.
    """
    if not data_list:
        return

    client = get_gcs_client()
    blob = client.bucket(bucket_name).blob(object_path)

    # Serialize records to Newline Delimited JSON (NDJSON)
    ndjson_content = "\n".join(
        [json.dumps(record, ensure_ascii=False) for record in data_list]
    )

    blob.upload_from_string(ndjson_content, content_type="application/x-ndjson")
    logger.info(
        f"Chunk successfully saved: gs://{bucket_name}/{object_path} ({len(data_list)} entries)"
    )


def upload_string_to_gcs(bucket_name, object_path, content, content_type="text/plain"):
    """
    Generic function to upload string content (CSV, JSON, plain text) to GCS.
    Leverages the centralized GCS client.
    """
    client = get_gcs_client()
    blob = client.bucket(bucket_name).blob(object_path)

    blob.upload_from_string(content, content_type=content_type)
    logger.info(f"Successfully uploaded to: gs://{bucket_name}/{object_path}")


def get_partition_path(logical_date_str):
    """
    Standardizes the conversion of a logical date string (YYYY-MM-DD)
    into a GCS hive-style or directory partition path (YYYY/MM/DD).
    Ensures path consistency across all data pipelines.
    """
    dt_obj = datetime.strptime(logical_date_str, "%Y-%m-%d")
    return f"{dt_obj.year}/{dt_obj.month:02d}/{dt_obj.day:02d}"
