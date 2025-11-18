from __future__ import annotations

import csv
import logging
import os
from typing import Any, Dict, Iterable, Iterator

try:  # pragma: no cover - optional dependency
    import gspread  # type: ignore
    from google.oauth2.service_account import Credentials  # type: ignore
except Exception:  # pragma: no cover - fallback when gspread unavailable
    gspread = None
    Credentials = None

from pydantic import ValidationError
from pymongo import MongoClient
from pymongo.collection import Collection
from pymongo.errors import OperationFailure

from src.config import Settings, get_settings
from src.models import Sekolah

logger = logging.getLogger(__name__)

SCOPES = ["https://www.googleapis.com/auth/spreadsheets.readonly"]


def _read_csv(path: str) -> Iterable[Dict[str, Any]]:
    if not os.path.exists(path):
        raise FileNotFoundError(f"CSV not found: {path}")
    with open(path, newline="", encoding="utf-8") as handle:
        yield from csv.DictReader(handle)


def _read_google_sheet(sheet_id: str, worksheet: str, credentials_path: str) -> Iterable[Dict[str, Any]]:
    if gspread is None:
        raise RuntimeError("Google Sheets support unavailable; install gspread or use CSV source.")
    if not os.path.exists(credentials_path):
        raise FileNotFoundError(f"Service account file missing: {credentials_path}")
    creds = Credentials.from_service_account_file(credentials_path, scopes=SCOPES)
    client = gspread.authorize(creds)
    sheet = client.open_by_key(sheet_id)
    worksheet_obj = sheet.worksheet(worksheet)
    yield from worksheet_obj.get_all_records()


def _load_rows(settings: Settings) -> Iterable[Dict[str, Any]]:
    source = settings.source.lower()
    if source == "csv":
        logger.info("Loading data from CSV: %s", settings.csv_path)
        return _read_csv(settings.csv_path)
    if source == "gsheet":
        if not settings.gsheet_id:
            raise ValueError("GSHEET_ID must be set when SOURCE is gsheet")
        logger.info(
            "Loading data from Google Sheet %s (%s)",
            settings.gsheet_id,
            settings.gsheet_worksheet_name,
        )
        return _read_google_sheet(
            settings.gsheet_id,
            settings.gsheet_worksheet_name,
            settings.google_credentials_path,
        )
    raise ValueError(f"Unsupported source '{settings.source}'")


def _iter_schools(settings: Settings) -> Iterator[Sekolah]:
    for index, row in enumerate(_load_rows(settings), start=1):
        try:
            yield Sekolah.model_validate(row)
        except ValidationError as exc:  # pragma: no cover - logging aid
            logger.warning("Row %s failed validation: %s", index, exc)


def _chunked(rows: Iterable[Dict[str, Any]], size: int) -> Iterator[list[Dict[str, Any]]]:
    if size <= 0:
        raise ValueError("batch size must be positive")
    batch: list[Dict[str, Any]] = []
    for item in rows:
        batch.append(item)
        if len(batch) >= size:
            yield batch
            batch = []
    if batch:
        yield batch


def _replace_collection(
    collection: Collection,
    documents: Iterable[Dict[str, Any]],
    *,
    batch_size: int,
    dry_run: bool,
) -> dict[str, int]:
    processed = 0
    inserted = 0
    if not dry_run:
        collection.delete_many({})

    for chunk in _chunked(documents, batch_size):
        processed += len(chunk)
        if dry_run or not chunk:
            continue
        collection.insert_many(chunk, ordered=False)
        inserted += len(chunk)

    return {"processed": processed, "inserted": inserted, "dry_run": int(dry_run)}


def _get_collection(settings: Settings) -> Collection:
    client = MongoClient(settings.mongo_uri)
    database = client[settings.db_name]
    return database[Sekolah.collection_name]


def run(settings: Settings) -> dict[str, int]:
    logger.info("Starting ingestion (dry_run=%s)", settings.dry_run)
    collection = _get_collection(settings)
    documents = (school.to_document() for school in _iter_schools(settings))

    try:
        result = _replace_collection(
            collection,
            documents,
            batch_size=settings.batch_size,
            dry_run=settings.dry_run,
        )
    except OperationFailure as exc:
        if exc.code == 13:
            logger.error(
                "MongoDB authentication failed. Ensure your MONGO_URI includes the correct credentials and authSource if required.")
            raise RuntimeError("MongoDB authentication failed; check credentials and auth source") from exc
        logger.error("MongoDB operation failed: %s", exc)
        raise

    logger.info("Ingestion finished: %s", result)
    return result


def run_with_overrides(**overrides: Any) -> dict[str, int]:
    settings = get_settings().model_copy(update=overrides)
    return run(settings)
