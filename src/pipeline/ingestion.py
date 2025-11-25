from __future__ import annotations

import csv
import logging
import os
from datetime import datetime, timezone
from typing import Any, Dict, Iterable

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


def _format_validation_messages(exc: ValidationError) -> list[str]:
    messages: list[str] = []
    for error in exc.errors():
        message = error.get("msg")
        if not message:
            continue
        prefix = "value error, "
        if message.lower().startswith(prefix):
            message = message[len(prefix):]
        messages.append(message)
    if not messages:
        messages.append(str(exc))
    return messages


def _utc_now() -> datetime:
    return datetime.now(timezone.utc)


def _canonicalise_document(doc: Dict[str, Any], *, reference_keys: Iterable[str] | None = None) -> Dict[str, Any]:
    canonical = {key: value for key, value in doc.items() if key not in {"_id", "createdAt", "updatedAt"}}
    if reference_keys is not None:
        for key in reference_keys:
            canonical.setdefault(key, None)
    return canonical


def _build_filter(existing: Dict[str, Any] | None, kod_sekolah: str) -> Dict[str, Any]:
    if existing and "_id" in existing:
        return {"_id": existing["_id"]}
    return {"kodSekolah": kod_sekolah}


def _merge_document(
    existing: Dict[str, Any] | None,
    prepared: Dict[str, Any],
    *,
    timestamp: datetime,
) -> Dict[str, Any]:
    if existing is None:
        document = dict(prepared)
        document["createdAt"] = timestamp
        document["updatedAt"] = timestamp
        return {"action": "insert", "document": document}

    existing_signature = _canonicalise_document(existing, reference_keys=prepared.keys())
    data_changed = existing_signature != prepared
    created_at = existing.get("createdAt")
    filter_query = _build_filter(existing, str(prepared["kodSekolah"]))

    if data_changed:
        return {
            "action": "update",
            "filter": filter_query,
            "set": dict(prepared),
        }

    if not created_at:
        repair_created_at = existing.get("updatedAt") or timestamp
        return {
            "action": "repair",
            "filter": filter_query,
            "set": {"createdAt": repair_created_at},
        }

    return {"action": "noop"}


def _index_existing_documents(collection: Collection) -> Dict[str, Dict[str, Any]]:
    existing: Dict[str, Dict[str, Any]] = {}
    for doc in collection.find({}):
        kod = doc.get("kodSekolah")
        if not kod:
            continue
        existing[str(kod)] = doc
    return existing


def _sync_sekolah_collection(settings: Settings, collection: Collection) -> dict[str, Any]:
    errors: list[dict[str, Any]] = []
    inserted = updated = repaired = unchanged = 0

    existing_index = _index_existing_documents(collection)
    seen: set[str] = set()

    for index, row in enumerate(_load_rows(settings), start=1):
        try:
            sekolah = Sekolah.model_validate(row)
        except ValidationError as exc:
            messages_list = _format_validation_messages(exc)
            messages = "; ".join(messages_list)
            errors.append({"row": index, "error": messages})
            continue

        prepared = sekolah.to_document(include_timestamps=False)
        kod = str(prepared["kodSekolah"])
        if kod in seen:
            errors.append({"row": index, "error": f"Duplicate kodSekolah '{kod}' in source data"})
            continue
        seen.add(kod)

        existing = existing_index.pop(kod, None)
        plan = _merge_document(existing, prepared, timestamp=_utc_now())
        action = plan["action"]

        if action == "insert":
            inserted += 1
            if not settings.dry_run:
                collection.insert_one(plan["document"])
            continue

        if action == "update":
            updated += 1
            if not settings.dry_run:
                collection.update_one(
                    plan["filter"],
                    {"$set": plan["set"], "$currentDate": {"updatedAt": True}},
                    upsert=False,
                )
            continue

        if action == "repair":
            repaired += 1
            if not settings.dry_run:
                collection.update_one(
                    plan["filter"],
                    {"$set": plan["set"], "$currentDate": {"updatedAt": True}},
                    upsert=False,
                )
            continue

        if action == "noop":
            unchanged += 1
            continue

    deleted = 0
    if existing_index:
        ids_to_delete = [doc["_id"] for doc in existing_index.values() if isinstance(doc, dict) and "_id" in doc]
        if ids_to_delete:
            deleted = len(ids_to_delete)
            if not settings.dry_run:
                collection.delete_many({"_id": {"$in": ids_to_delete}})

    processed = inserted + updated + repaired + unchanged
    total = processed + len(errors)

    return {
        "total": total,
        "processed": processed,
        "failed": len(errors),
        "errors": errors,
        "inserted": inserted,
        "updated": updated,
        "repaired": repaired,
        "unchanged": unchanged,
        "deleted": deleted,
    }


def _get_collection(settings: Settings) -> Collection:
    client = MongoClient(settings.mongo_uri)
    database = client[settings.sekolah_collection]
    return database[Sekolah.collection_name]


def run(settings: Settings) -> dict[str, Any]:
    logger.info("Starting ingestion (dry_run=%s)", settings.dry_run)
    collection = _get_collection(settings)

    try:
        result = _sync_sekolah_collection(settings, collection)
    except OperationFailure as exc:
        if exc.code == 13:
            logger.error(
                "MongoDB authentication failed. Ensure your MONGO_URI includes the correct credentials and authSource if required.")
            raise RuntimeError("MongoDB authentication failed; check credentials and auth source") from exc
        logger.error("MongoDB operation failed: %s", exc)
        raise

    result.update(
        {
            "collection": Sekolah.collection_name,
            "dry_run": settings.dry_run,
        }
    )
    return result


def run_with_overrides(**overrides: Any) -> dict[str, Any]:
    settings = get_settings().model_copy(update=overrides)
    return run(settings)
