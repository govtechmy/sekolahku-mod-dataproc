import requests
import logging
import re
import urllib.parse

logger = logging.getLogger(__name__)

def _extract_filename(content_disp: str) -> str | None:
    if not content_disp:
        return None

    # RFC 5987 format: filename*=UTF-8''encoded_name.csv
    match_star = re.search(r"filename\*=UTF-8''([^;]+)(?:;|$)", content_disp, re.IGNORECASE,)
    if match_star:
        filename = urllib.parse.unquote(match_star.group(1))
        logger.info("Extracted filename: %s", filename)
        return filename

    # Standard format: filename="file.csv" OR filename=file.csv
    match = re.search(r'filename=(?:"([^"]*)"|([^;]+))(?:;|$)', content_disp, re.IGNORECASE,)
    if match:
        filename = match.group(1) if match.group(1) is not None else match.group(2).strip()
        logger.info("Extracted filename (standard): %s", filename)
        return filename

    logger.warning("Failed to extract filename from Content-Disposition: %s", content_disp)
    return None

def _extract_file_version(file_name: str | None) -> str | None:
    if not file_name:
        return None

    name = file_name.strip().rsplit(".", 1)[0]
    name = name.split(" - ")[0]
    parts = name.split("_")

    if len(parts) >= 2:
        file_version = parts[-1]
        logger.info("Extracted fileVersion: %s", file_version)
        return file_version

    logger.warning("Failed to extract fileVersion from filename: %s", file_name)
    return None

def fetch_csv_data(gsheet_id: str, gid: str) -> tuple[bytes, str | None]:
    url = f"https://docs.google.com/spreadsheets/d/{gsheet_id}/export?format=csv&gid={gid}"
    logger.info("Fetching CSV data from URL: %s", url)

    response = requests.get(url)
    response.raise_for_status()

    content_type = response.headers.get("Content-Type", "")
    if "text/csv" not in content_type:
        logger.warning("Google Sheet not accessible as CSV. Content-Type: %s", content_type)

    content_disp = response.headers.get("Content-Disposition", "")
    filename = _extract_filename(content_disp)

    return response.content, filename