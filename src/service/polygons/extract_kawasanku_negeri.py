import csv
import json
import logging
import os
import requests
import boto3
from urllib.parse import unquote_plus
from src.config.settings import get_settings

# Load environment variables
logger = logging.getLogger(__name__)

# Get settings instance
settings = get_settings()

# NEGERI_CSV_PATH = os.getenv("NEGERI_CSV_PATH")
# RAW_DIR = "data_output/raw_negeri"
# EXTRACTED_DIR = "data_output/extracted_negeri"

NEGERI_CSV_PATH = "src/service/polygons/data/url_negeri.csv"

# S3 Configuration from environment
S3_BUCKET = settings.s3_bucket_dataproc
S3_PREFIX_OPENDOSM = "opendosm/negeri/"
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
    Extract and format filename as: STATE.json
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
        logger.info(f"Uploading to S3: s3://{settings.s3_bucket_dataproc}/{s3_key}")
        s3_client.upload_file(local_path, settings.s3_bucket_dataproc, s3_key)
        logger.info(f"Successfully uploaded to S3")
        return True
    except Exception as e:
        logger.error(f"S3 upload failed: {e}")
        return False


def main():
    # No need to create local directories - uploading directly to S3

    # Initialize S3 client with AWS profile
    logger.info("Initializing S3 client...")
    s3_client = boto3.client('s3')
    logger.info(f"Connected to S3 bucket: {settings.s3_bucket_dataproc}")

    # Read CSV
    with open(NEGERI_CSV_PATH, newline="", encoding="utf-8-sig") as f:
        reader = csv.reader(f)
        rows = list(reader)

    if not rows:
        raise SystemExit("CSV is empty.")

    header = rows[0][0].strip().lower()
    if header != "url":
        raise SystemExit("CSV must have a single header named 'url'.")

    urls = [row[0].strip() for row in rows[1:] if row and row[0].strip()]

    # ---------------------------------------------------------
    # STEP 1 — Download and upload directly to S3
    # ---------------------------------------------------------
    for url in urls:
        filename = extract_filename(url)

        logger.info("\n==============================================================")
        logger.info(f"Downloading: {url}")
        logger.info(f"Uploading to S3: s3://{settings.s3_bucket_dataproc}/{S3_PREFIX_OPENDOSM}{filename}")
        logger.info("==============================================================")

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
            logger.info(f"✓ Successfully uploaded to S3")
        except Exception as e:
            logger.error(f"✗ S3 upload failed: {e}")
            continue

    # ---------------------------------------------------------
    # STEP 2 — Load from S3 and extract required fields
    # ---------------------------------------------------------
    for url in urls:
        filename = extract_filename(url)
        s3_raw_key = S3_PREFIX_OPENDOSM + filename

        logger.info(f"Extracting from S3: s3://{settings.s3_bucket_dataproc}/{s3_raw_key}")
        
        # Load JSON directly from S3
        try:
            response = s3_client.get_object(Bucket=settings.s3_bucket_dataproc, Key=s3_raw_key)
            root = json.loads(response['Body'].read().decode('utf-8'))
        except Exception as e:
            logger.error(f"Failed to read from S3: {e}")
            continue

        pageProps = root.get("pageProps", {})
        params = pageProps.get("params", {})
        geojson = pageProps.get("geojson", {})

        logger.info("\nSCHEMA: pageProps.params")
        print_schema(params)

        logger.info("\nSCHEMA: pageProps.geojson")
        print_schema(geojson)

        extracted_data = {
            "params": params,
            "geojson": geojson
        }
        
        # TODO: Uncomment to upload extracted JSON to S3 (requires S3_EXTRACTED_PREFIX)
        # s3_extract_key = S3_EXTRACTED_PREFIX + filename
        # upload_to_s3(s3_client, extract_path, s3_extract_key)


if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(levelname)s - %(message)s'
    )
    main()


