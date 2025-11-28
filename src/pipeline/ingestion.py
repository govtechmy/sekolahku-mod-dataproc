from __future__ import annotations

import csv
import logging
import os
from datetime import datetime, timezone
from typing import Any, Dict, Iterable, Iterator

try:  # pragma: no cover - optional dependency
    import gspread  # type: ignore
    from google.oauth2.service_account import Credentials  # type: ignore
except Exception:  # pragma: no cover - fallback when gspread unavailable
    gspread = None
    Credentials = None

from pydantic import ValidationError
from pymongo import MongoClient, UpdateOne
from pymongo.collection import Collection
from pymongo.errors import OperationFailure

from src.config import Settings, get_settings
from src.models import Sekolah
from src.models.sekolah import SekolahStatus
from src.pipeline.status_sync import sync_entiti_statuses
def _utc_now() -> datetime:
    return datetime.now(timezone.utc)


def _merge_document(
    existing: dict[str, Any] | None,
    payload: dict[str, Any],
    *,
    timestamp: datetime | None = None,
) -> dict[str, Any]:
    """
    New documents get both createdAt and updatedAt set to the given (or current) timestamp.
    Existing documents update only changed fields and let MongoDB refresh updatedAt via $currentDate.
    """

    ts = timestamp or _utc_now()
    next_payload = dict(payload)
    next_payload.pop("_id", None)

    if existing is None:
        document = dict(next_payload)
        document["createdAt"] = ts
        document["updatedAt"] = ts
        return {
            "action": "insert",
            "document": document,
        }

    changes: dict[str, Any] = {}
    for key, value in next_payload.items():
        if existing.get(key) != value:
            changes[key] = value

    if not changes:
        return {"action": "noop"}

    return {
        "action": "update",
        "filter": {"_id": existing.get("_id")},
        "set": changes,
        "currentDate": {"updatedAt": True},
    }

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


def _collect_documents(
    settings: Settings,
) -> tuple[list[dict[str, Any]], list[dict[str, Any]], int]:
    documents: list[dict[str, Any]] = []
    errors: list[dict[str, Any]] = []
    total = 0

    for index, row in enumerate(_load_rows(settings), start=1):
        total += 1
        try:
            sekolah = Sekolah.model_validate(row)
        except ValidationError as exc:  # pragma: no cover - logging aid
            messages_list = _format_validation_messages(exc)
            messages = "; ".join(messages_list)
            errors.append({"row": index, "error": messages})
            continue

        documents.append(sekolah.to_document())

    return documents, errors, total


def _replace_collection(
    collection: Collection,
    documents: Iterable[Dict[str, Any]],
    *,
    batch_size: int,
    dry_run: bool,
) -> dict[str, int]:
    if batch_size <= 0:
        raise ValueError("batch size must be positive")

    processed = 0
    inserted = 0
    updated = 0
    skipped = 0

    for chunk in _chunked(documents, batch_size):
        identifiers: list[Any] = []
        chunk_documents: list[tuple[Any, Dict[str, Any]]] = []

        for document in chunk:
            identifier = document.get("_id") or document.get("kodSekolah")
            if identifier is None:
                skipped += 1
                logger.warning("Skipping document without identifier: %s", document)
                continue

            processed += 1
            identifiers.append(identifier)
            chunk_documents.append((identifier, document))

        if not chunk_documents:
            continue

        existing_map: dict[Any, dict[str, Any]] = {}
        if identifiers:
            existing_cursor = collection.find({"_id": {"$in": identifiers}})
            existing_map = {doc["_id"]: doc for doc in existing_cursor}

        operations: list[UpdateOne] = []
        for identifier, document in chunk_documents:
            existing = existing_map.get(identifier)

            comparable_fields = {
                key: value
                for key, value in document.items()
                if key not in {"_id", "createdAt", "updatedAt"}
            }

            if existing is None:
                changes = comparable_fields
            else:
                changes = {
                    key: value
                    for key, value in comparable_fields.items()
                    if existing.get(key) != value
                }

            if existing is not None and not changes:
                continue

            created_at_on_insert = (
                (existing.get("createdAt") if existing else None)
                or document.get("createdAt")
                or _utc_now()
            )

            update_document: dict[str, Any] = {
                "$set": changes,
                "$setOnInsert": {"createdAt": created_at_on_insert},
                "$currentDate": {"updatedAt": True},
            }

            operations.append(
                UpdateOne(
                    {"_id": identifier},
                    update_document,
                    upsert=True,
                )
            )

        if dry_run or not operations:
            continue

        result = collection.bulk_write(operations, ordered=False)
        inserted += getattr(result, "upserted_count", 0) or 0
        updated += getattr(result, "modified_count", 0) or 0

    return {
        "processed": processed,
        "inserted": inserted,
        "updated": updated,
        "skipped": skipped,
        "dry_run": dry_run,
    }


def _mark_missing_schools_inactive(
    collection: Collection,
    active_identifiers: set[Any],
    *,
    dry_run: bool,
) -> int:
    if dry_run:
        if hasattr(collection, "count_documents"):
            selector: Dict[str, Any] = {"status": SekolahStatus.ACTIVE.value}
            if active_identifiers:
                selector["_id"] = {"$nin": list(active_identifiers)}
            return int(collection.count_documents(selector))
        return 0

    selector = {"status": SekolahStatus.ACTIVE.value}
    if active_identifiers:
        selector["_id"] = {"$nin": list(active_identifiers)}

    update_document = {
        "$set": {"status": SekolahStatus.INACTIVE.value},
        "$currentDate": {"updatedAt": True},
    }

    result = collection.update_many(selector, update_document)
    modified = getattr(result, "modified_count", None)
    if modified is not None:
        return int(modified)
    matched = getattr(result, "matched_count", 0)
    return int(matched)


def _get_database(settings: Settings):
    client = MongoClient(settings.mongo_uri)
    return client[settings.db_name]


def run(settings: Settings) -> dict[str, Any]:
    logger.info("Starting ingestion (dry_run=%s)", settings.dry_run)
    database = _get_database(settings)
    sekolah_collection = database[Sekolah.collection_name]
    entiti_collection = database[settings.entiti_sekolah_collection]
    documents, errors, total = _collect_documents(settings)

    active_identifiers: set[Any] = set()
    for document in documents:
        identifier = document.get("_id") or document.get("kodSekolah")
        if identifier is None:
            continue
        document["status"] = SekolahStatus.ACTIVE.value # All schools present in raw file are ACTIVE
        active_identifiers.add(identifier)

    try:
        result = _replace_collection(
            sekolah_collection,
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

    inactivated = _mark_missing_schools_inactive(
        sekolah_collection,
        active_identifiers,
        dry_run=settings.dry_run,
    )
    if settings.dry_run:
        logger.info("Dry run: %s sekolah would be marked inactive", inactivated)
    else:
        logger.info("Marked %s sekolah as inactive", inactivated)

    entiti_synced = sync_entiti_statuses(
        sekolah_collection,
        entiti_collection,
        batch_size=settings.batch_size,
        dry_run=settings.dry_run,
    )
    if settings.dry_run:
        logger.info("Dry run: %s EntitiSekolah documents would be synced", entiti_synced)
    else:
        logger.info("Synced %s EntitiSekolah statuses", entiti_synced)

    summary = {
        "collection": Sekolah.collection_name,
        "total": total,
        "processed": result["processed"],
        "failed": len(errors),
        "errors": errors,
        "inserted": result["inserted"],
        "updated": result["updated"],
        "skipped": result["skipped"],
        "dry_run": result["dry_run"],
        "inactivated": inactivated,
        "entiti_synced": entiti_synced,
    }
    return summary


def run_with_overrides(**overrides: Any) -> dict[str, Any]:
    settings = get_settings().model_copy(update=overrides)
    return run(settings)
