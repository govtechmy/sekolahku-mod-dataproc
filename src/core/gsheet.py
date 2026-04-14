import requests
import logging
import re
import urllib.parse

logger = logging.getLogger(__name__)

def _extract_filename(content_disp: str) -> str | None:
    match_star = re.search(r"filename\*\=UTF-8''(.+)", content_disp)
    if match_star:
        return urllib.parse.unquote(match_star.group(1))

    match = re.search(r'filename="(.+)"', content_disp)
    if match:
        return match.group(1)

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