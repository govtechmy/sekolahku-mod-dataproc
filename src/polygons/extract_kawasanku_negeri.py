import csv
import json
import os
import requests
import boto3
from urllib.parse import unquote_plus
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

CSV_PATH = "/Users/mydigital/Documents/Github/sekolahku-mod-dataproc/src/polygons/data_input/url_negeri.csv"
RAW_DIR = "data_output/raw_negeri"
EXTRACTED_DIR = "data_output/extracted_negeri"

# S3 Configuration from environment
S3_BUCKET = os.getenv("S3_BUCKET")
S3_REGION = os.getenv("S3_REGION")
AWS_PROFILE = os.getenv("AWS_PROFILE")
S3_RAW_PREFIX = "opendosm/raw/negeri/"
# S3_EXTRACTED_PREFIX = "opendosm/extracted/negeri/"


def print_schema(obj, indent=0):
    pad = " " * indent
    if isinstance(obj, dict):
        for key, value in obj.items():
            print(f"{pad}{key}: {type(value).__name__}")
            print_schema(value, indent + 2)
    elif isinstance(obj, list):
        print(f"{pad}List[{len(obj)}]")
        if obj:
            print_schema(obj[0], indent + 2)
    else:
        print(f"{pad}{type(obj).__name__}")


def extract_filename(url: str) -> str:
    """
    Extract and format filename as: STATE.json
    Example: PULAU_PINANG.json, NEGERI_SEMBILAN.json
    
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
        print(f"  ☁️  Uploading to S3: s3://{S3_BUCKET}/{s3_key}")
        s3_client.upload_file(local_path, S3_BUCKET, s3_key)
        print(f"  ✅ Successfully uploaded to S3")
        return True
    except Exception as e:
        print(f"  ❌ S3 upload failed: {e}")
        return False


def main():
    # Create folders (local backup)
    os.makedirs(RAW_DIR, exist_ok=True)
    os.makedirs(EXTRACTED_DIR, exist_ok=True)

    # Initialize S3 client with AWS profile
    print("Initializing S3 client...")
    session = boto3.Session(profile_name=AWS_PROFILE)
    s3_client = session.client('s3', region_name=S3_REGION)
    print(f"✅ Connected to S3 bucket: {S3_BUCKET}\n")

    # Read CSV (1 header: url)
    with open(CSV_PATH, newline="", encoding="utf-8-sig") as f:
        reader = csv.reader(f)
        rows = list(reader)

    if not rows:
        raise SystemExit("CSV is empty.")

    header = rows[0][0].strip().lower()
    if header != "url":
        raise SystemExit("CSV must have a single header named 'url'.")

    urls = [row[0].strip() for row in rows[1:] if row and row[0].strip()]

    # ---------------------------------------------------------
    # STEP 1 — Download full JSON into raw folder
    # ---------------------------------------------------------
    for url in urls:
        filename = extract_filename(url)
        save_path = os.path.join(RAW_DIR, filename)

        print("\n==============================================================")
        print("Downloading:", url)
        print("Saving raw JSON →", save_path)
        print("==============================================================")

        try:
            resp = requests.get(url, timeout=20)
            resp.raise_for_status()
            data = resp.json()
        except Exception as e:
            print("❌ ERROR:", e)
            continue

        # Save raw JSON locally
        with open(save_path, "w", encoding="utf-8") as f:
            json.dump(data, f, indent=2, ensure_ascii=False)
        
        # Upload raw JSON to S3
        s3_raw_key = S3_RAW_PREFIX + filename
        upload_to_s3(s3_client, save_path, s3_raw_key)

    # ---------------------------------------------------------
    # STEP 2 — Load saved file and extract required fields
    # ---------------------------------------------------------
    for url in urls:
        filename = extract_filename(url)
        raw_path = os.path.join(RAW_DIR, filename)
        extract_path = os.path.join(EXTRACTED_DIR, filename)

        print("\nExtracting from:", raw_path)

        # Load the already downloaded JSON
        with open(raw_path, "r", encoding="utf-8") as f:
            root = json.load(f)

        pageProps = root.get("pageProps", {})
        params = pageProps.get("params", {})
        geojson = pageProps.get("geojson", {})

        # Print schema only for the two sections
        print("\nSCHEMA: pageProps.params")
        print_schema(params)

        print("\nSCHEMA: pageProps.geojson")
        print_schema(geojson)

        # Save extracted JSON
        extracted_data = {
            "params": params,
            "geojson": geojson
        }

        with open(extract_path, "w", encoding="utf-8") as f:
            json.dump(extracted_data, f, indent=2, ensure_ascii=False)

        print("Saved extracted JSON →", extract_path)
        
        # Upload extracted JSON to S3
        # s3_extract_key = S3_EXTRACTED_PREFIX + filename
        # upload_to_s3(s3_client, extract_path, s3_extract_key)


if __name__ == "__main__":
    main()
