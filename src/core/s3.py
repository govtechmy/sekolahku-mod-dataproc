import time
from typing import Optional, List

import pandas as pd

from src.core.aws import get_s3_client, get_s3_bucket_name

s3 = get_s3_client()

def _upload_to_s3(csv_bytes: bytes, bucket: str, prefix: str) -> str:
    if not bucket:
        bucket = get_s3_bucket_name()

    timestamp = int(time.time())
    s3_key = f"{prefix}/{timestamp}.csv"

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
    return pd.read_csv(response["Body"])