import csv
import json
import logging
import os
from concurrent.futures import ThreadPoolExecutor, as_completed

import requests
import boto3
from urllib.parse import unquote_plus
from src.config.settings import get_settings

logger = logging.getLogger(__name__)

settings = get_settings()

NEGERI_CSV_PATH = "src/service/polygons/data/url_negeri.csv"

S3_BUCKET = settings.s3_bucket_dataproc
S3_PREFIX_OPENDOSM = f"{settings.s3_prefix_opendosm}/negeri/"
# TODO: Uncomment when ready to upload extracted files to S3
# S3_EXTRACTED_PREFIX = "opendosm/extracted/negeri/"

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
    Extract and format filename as: STATE.json.

    Example: NEGERI_SEMBILAN.json

    URL format: .../kawasanku/Negeri%20Sembilan.json?state=Negeri%20Sembilan
    """
    # Extract state from query parameter and decode URL encoding
    state_param = url.split("state=")[-1].split("&")[0]
    # Decode URL encoding: %20 and + both become spaces, then replace spaces with _
    state = unquote_plus(state_param).upper().replace(" ", "_")
    return f"{state}.json"


def upload_to_s3(s3_client, local_path: str, s3_key: str) -> bool:
    """Upload a file to S3 bucket."""
    try:
        logger.debug(f"Uploading to S3: s3://{settings.s3_bucket_dataproc}/{s3_key}")
        s3_client.upload_file(local_path, settings.s3_bucket_dataproc, s3_key)
        return True
    except Exception as e:
        logger.error(f"S3 upload failed: {e}")
        return False


def _fetch_and_upload(s3_client, url: str) -> tuple[bool, str]:
    """Fetch a single URL and upload its JSON to S3.

    Returns (success, url) so caller can update counters.
    """
    filename = extract_filename(url)
    logger.debug("Downloading: %s", url)
    logger.debug("Uploading to S3: s3://%s/%s%s", settings.s3_bucket_dataproc, S3_PREFIX_OPENDOSM, filename)

    try:
        resp = requests.get(url, timeout=20)
        resp.raise_for_status()
        data = resp.json()
    except Exception as e:  # noqa: BLE001
        logger.error("ERROR fetching %s: %s", url, e)
        return False, url

    s3_raw_key = S3_PREFIX_OPENDOSM + filename
    try:
        s3_client.put_object(
            Bucket=settings.s3_bucket_dataproc,
            Key=s3_raw_key,
            Body=json.dumps(data, indent=2, ensure_ascii=False),
            ContentType="application/json",
        )
        return True, url
    except Exception as e:  # noqa: BLE001
        logger.error("S3 upload failed for %s: %s", url, e)
        return False, url


def main():
    s3_client = boto3.client("s3")

    with open(NEGERI_CSV_PATH, newline="", encoding="utf-8-sig") as f:
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
    logger.info("Downloading and uploading to S3 in progress...")

    # Counters for tracking
    upload_success = 0
    upload_failed = 0

    # Use thread workers for I/O-bound HTTP+S3 operations
    with ThreadPoolExecutor(max_workers=settings.entiti_revalidate_max_workers) as executor:
        future_to_url = {executor.submit(_fetch_and_upload, s3_client, url): url for url in urls}

        for future in as_completed(future_to_url):
            success, url = future.result()
            if success:
                upload_success += 1
            else:
                upload_failed += 1

    logger.info(f"Upload successful: {upload_success}, failed: {upload_failed}")


if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(levelname)s - %(message)s",
    )
    main()


