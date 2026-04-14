import json
import logging
import time
from typing import Optional, List
import pandas as pd
import re
from botocore.exceptions import ClientError, ResponseStreamingError

from src.core.aws import get_s3_client, get_s3_bucket_name

s3 = get_s3_client()
logger = logging.getLogger(__name__)

def _upload_to_s3(csv_bytes: bytes, bucket: str, prefix: str, source_filename: str) -> str:
    if not bucket:
        bucket = get_s3_bucket_name()

    timestamp = int(time.time())
    version = source_filename.split(" - ")[0].replace(".xlsx", "").replace(".csv", "") if source_filename else "unknown"
    s3_key = f"{prefix}/{version}/{timestamp}.csv"

    s3.put_object(
        Bucket=bucket,
        Key=s3_key,
        Body=csv_bytes,
        ContentType="text/csv"
    )

    return s3_key

def _list_csv_files_in_s3(bucket: str, prefix: str) -> list[str]:
    response = s3.list_objects_v2(Bucket=bucket, Prefix=prefix)

    if "Contents" not in response:
        return []

    return sorted (
        [item["Key"] for item in response["Contents"] if item["Key"].endswith(".csv")]
    )

def _latest_csv_from_s3(bucket: str, prefix: str) -> Optional[str]:
    csv_files = _list_csv_files_in_s3(bucket, prefix)
    return csv_files[-1] if csv_files else None

def _read_csv_from_s3(bucket: str, s3_key: str) -> pd.DataFrame:
    response = s3.get_object(Bucket=bucket, Key=s3_key)
    return pd.read_csv(response["Body"], dtype=str).fillna("")

def upload_json_to_s3(payload: dict | list, bucket: Optional[str], key: str) -> str:
    """
    Upload JSON to a fixed S3 key (no timestamp).
    Used for:
      - common/snap-routes.json
      - common/school-list.json
    """
    if not bucket:
        bucket = get_s3_bucket_name()

    # minify JSON
    body = json.dumps(payload, ensure_ascii=False, separators=(',', ':')).encode("utf-8")

    s3.put_object(
        Bucket=bucket,
        Key=key,
        Body=body,
        ContentType="application/json; charset=utf-8"
    )

    return key

def read_json_from_s3(bucket: str, key: str, *, max_retries: int = 2) -> dict | None:
    """Read and parse a JSON object from S3 with a small retry on streaming errors."""

    s3 = get_s3_client()
    attempt = 0

    while True:
        attempt += 1
        try:
            obj = s3.get_object(Bucket=bucket, Key=key)
            body = obj["Body"].read()
            return json.loads(body)
        except ResponseStreamingError as e:
            # Log and retry a couple of times for transient network issues
            logger.warning("Streaming error while reading s3://%s/%s (attempt %d/%d): %s", bucket, key, attempt, max_retries, e)
            if attempt >= max_retries:
                logger.error("Giving up reading s3://%s/%s after %d attempts due to streaming errors", bucket, key, max_retries,)
                return None

            time.sleep(0.5 * attempt)
            continue
        except ClientError as e:
            logger.warning("Error reading S3 object %s from bucket %s: %s", key, bucket, e)
            return None
