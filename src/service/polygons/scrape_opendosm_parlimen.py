import csv
import json
import logging
import os
import requests
import boto3
from urllib.parse import unquote_plus
from src.config.settings import get_settings

logger = logging.getLogger(__name__)

settings = get_settings()

PARLIMEN_CSV_PATH = "src/service/polygons/data/url_parlimen.csv"

S3_BUCKET = settings.s3_bucket_dataproc
S3_PREFIX_OPENDOSM = f"{settings.s3_prefix_opendosm}/parlimen/"
# TODO: Uncomment when ready to upload extracted files to S3
# S3_EXTRACTED_PREFIX = "opendosm/extracted/parlimen/"

_settings = get_settings()

def print_schema(obj, indent=0):
    pad = " " * indent
    if isinstance(obj, dict):
        for key, value in obj.items():
            logger.info(f"{pad}{key}: {type(value).__name__}")
            print_schema(value, indent + 2)
    elif isinstance(obj, list):
        logger.info(f"{pad}List[{len(obj)}]")
        if obj:
            print_schema(obj[0], indent + 2)
    else:
        logger.info(f"{pad}{type(obj).__name__}")


def extract_filename(url: str) -> str:
    """
    Extract and format filename as: STATE_P.XXX_NAME.json
    Example: PULAU_PINANG_P.044_TANJONG.json

    URL format: ...kawasanku/Kedah/parlimen/P.010%20Kuala%20Kedah.json?state=Kedah&id=P.010+Kuala+Kedah
    """
    # Extract state from query parameter and decode URL encoding
    state_param = url.split("state=")[-1].split("&")[0]
    # Decode URL encoding: %20 and + both become spaces, then replace spaces with _    
    state = unquote_plus(state_param).upper().replace(" ", "_")

    filename = url.split("/")[-1].split("?")[0]
    filename_no_ext = unquote_plus(filename.replace(".json", ""))

    parts = filename_no_ext.split(" ", 1)
    if len(parts) == 2:
        code = parts[0]
        name = parts[1].replace(" ", "_").upper()
        return f"{state}_{code}_{name}.json"
    else:
        return filename_no_ext.replace(" ", "_").upper() + ".json"


def upload_to_s3(s3_client, local_path: str, s3_key: str) -> bool:
    """Upload a file to S3 bucket."""
    try:
        logger.debug(f"Uploading to S3: s3://{settings.s3_bucket_dataproc}/{s3_key}")
        s3_client.upload_file(local_path, settings.s3_bucket_dataproc, s3_key)
        return True
    except Exception as e:
        logger.error(f"S3 upload failed: {e}")
        return False


def main():
    s3_client = boto3.client('s3')

    with open(PARLIMEN_CSV_PATH, newline="", encoding="utf-8-sig") as f:
        reader = csv.reader(f)
        rows = list(reader)

    if not rows:
        raise SystemExit("CSV is empty.")

    header = rows[0][0].strip().lower()
    if header != "url":
        raise SystemExit("CSV must have a single header named 'url'.")

    urls = [row[0].strip() for row in rows[1:] if row and row[0].strip()]
    total_urls = len(urls)
    logger.info(f"Total URLs to download: {total_urls}")
    logger.info(f"Downloading and uploading to S3 in progress...")

    # Counters for tracking
    upload_success = 0
    upload_failed = 0

    # Download and upload directly to S3
    for url in urls:
        filename = extract_filename(url)
        logger.debug(f"Downloading: {url}")
        logger.debug(f"Uploading to S3: s3://{settings.s3_bucket_dataproc}/{S3_PREFIX_OPENDOSM}{filename}")

        try:
            resp = requests.get(url, timeout=20)
            resp.raise_for_status()
            data = resp.json()
        except Exception as e:
            logger.error(f"ERROR: {e}")
            continue

        # Upload JSON directly to S3 (no local file)
        s3_raw_key = S3_PREFIX_OPENDOSM + filename
        try:
            s3_client.put_object(
                Bucket=settings.s3_bucket_dataproc,
                Key=s3_raw_key,
                Body=json.dumps(data, indent=2, ensure_ascii=False),
                ContentType='application/json'
            )
            upload_success += 1
        except Exception as e:
            logger.error(f"S3 upload failed: {e}")
            upload_failed += 1
            continue

    logger.info(f"Upload successful: {upload_success}, failed: {upload_failed}")


if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(levelname)s - %(message)s'
    )
    main()
