from typing import Iterable, Dict, Any
import os

try:
    import gspread  # type: ignore
    from google.oauth2.service_account import Credentials  # type: ignore
except Exception:  # pragma: no cover
    gspread = None
    Credentials = None

SCOPES = ["https://www.googleapis.com/auth/spreadsheets.readonly"]

def load(sheet_id: str, worksheet_name: str, credentials_path: str) -> Iterable[Dict[str, Any]]:
    if gspread is None:
        raise RuntimeError("gspread not installed; switch source to csv.")
    if not os.path.exists(credentials_path):
        raise FileNotFoundError(f"Service account file missing: {credentials_path}")
    creds = Credentials.from_service_account_file(credentials_path, scopes=SCOPES)
    gc = gspread.authorize(creds)
    sh = gc.open_by_key(sheet_id)
    ws = sh.worksheet(worksheet_name)
    for row in ws.get_all_records():
        yield row
