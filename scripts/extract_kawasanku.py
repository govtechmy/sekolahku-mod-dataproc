import csv
import json
import os
import requests

CSV_PATH = "data_input/url_negeri.csv"
RAW_DIR = "data_output/raw"
EXTRACTED_DIR = "data_output/extracted"


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
    """Extract <STATE>.json from URL."""
    return url.split("/")[-1].split("?")[0]


def main():
    # Create folders
    os.makedirs(RAW_DIR, exist_ok=True)
    os.makedirs(EXTRACTED_DIR, exist_ok=True)

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

        # Save raw JSON
        with open(save_path, "w", encoding="utf-8") as f:
            json.dump(data, f, indent=2, ensure_ascii=False)

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


if __name__ == "__main__":
    main()
