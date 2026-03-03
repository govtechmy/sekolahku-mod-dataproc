from __future__ import annotations

import csv
import hashlib
import json
import logging
import os
from datetime import datetime, timezone
import io 
import pandas as pd
from typing import Any, Dict, Iterable, Iterator, Set

from pydantic import ValidationError
from pymongo import MongoClient, UpdateOne
from pymongo.collection import Collection
from pymongo.errors import OperationFailure

from src.config import Settings, get_settings
from src.models import Sekolah
from src.core.gsheet import fetch_csv_data
from src.core.s3 import (_upload_to_s3, _latest_csv_from_s3, _read_csv_from_s3)
from src.models.sekolah import SekolahStatus
from src.pipeline.status_sync import sync_entiti_statuses
from src.core.time import _utc_now


CHECKSUM_EXCLUDE_KEYS = {"_id", "createdAt", "updatedAt", "checksum", "status"}
COMPARISON_EXCLUDE_KEYS = {"_id", "createdAt", "updatedAt"}

def _compute_checksum(document: Dict[str, Any]) -> str:
    filtered = {
        key: document[key]
        for key in sorted(document)
        if key not in CHECKSUM_EXCLUDE_KEYS
    }
    serialized = json.dumps(filtered, sort_keys=True, separators=(",", ":"), ensure_ascii=False,)
    return hashlib.sha256(serialized.encode("utf-8")).hexdigest()


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


def _read_csv(path: str) -> Iterable[Dict[str, Any]]:
    if not os.path.exists(path):
        # raise FileNotFoundError(f"CSV not found: {path}")
        logging.warning(f"CSV not found at {path}. Skipping ingestion.")
        return []
    with open(path, newline="", encoding="utf-8") as handle:
        yield from csv.DictReader(handle)

def _read_google_sheet(sheet_id: str, gid: str) -> Iterable[Dict[str, Any]]:
    logger.info("Scraping Google Sheet directly (sheet_id=%s, gid=%s)", sheet_id, gid,)

    csv_bytes = fetch_csv_data(sheet_id, gid)
    df = pd.read_csv(io.BytesIO(csv_bytes), dtype=str).fillna("")

    logger.info("Google Sheet loaded: %d rows, %d columns", df.shape[0], df.shape[1])
    return df.to_dict(orient="records")


def _load_rows(settings: Settings) -> Iterable[Dict[str, Any]]:
    csv_bytes = fetch_csv_data(settings.gsheet_id, settings.gsheet_gid)
    logger.info("Uploading CSV data to S3 bucket %s", settings.s3_bucket_dataproc)
    s3_key = _upload_to_s3(csv_bytes, settings.s3_bucket_dataproc, settings.s3_prefix_sekolah)
    logger.info("CSV uploaded to S3 at key: %s", s3_key)

    df = _read_csv_from_s3(settings.s3_bucket_dataproc, s3_key)
    logger.info("CSV loaded from S3: %d rows, %d columns", df.shape[0], df.shape[1])
    return df.to_dict(orient="records")


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
        loc = error.get("loc", [])
        field = ".".join(str(x) for x in loc)
        message = error.get("msg")
        if not message:
            continue
        prefix = "value error, "
        if message.lower().startswith(prefix):
            message = message[len(prefix):]
        messages.append(f"{field}: {message}")
    if not messages:
        messages.append(str(exc))
    return messages 


def _collect_documents(
    settings: Settings,
    kodSekolah_madani: Set[str],
) -> tuple[list[dict[str, Any]], list[dict[str, Any]], int, set[Any]]:
    documents: list[dict[str, Any]] = []
    errors: list[dict[str, Any]] = []
    total = 0
    present_identifiers: set[Any] = set()

    for index, row in enumerate(_load_rows(settings), start=1):
        total += 1

        raw_kod = str(row.get("KODSEKOLAH", "")).strip()
        if raw_kod:
            present_identifiers.add(raw_kod)

        try:
            sekolah = Sekolah.model_validate(row)
        except ValidationError as exc:
            # Other validation errors behave as before
            messages_list = _format_validation_messages(exc)
            messages = "; ".join(messages_list)
            errors.append({"row": index, "error": messages})
            continue

        document = sekolah.to_document()
        document["isSekolahAngkatMADANI"] = raw_kod in kodSekolah_madani
        documents.append(document)

    return documents, errors, total, present_identifiers


def _load_kodSekolah_madani(database, settings: Settings) -> Set[str]:
    collection = database[settings.sekolah_angkat_madani_collection]
    codes: Set[str] = set()

    try:
        cursor = collection.find({}, {"_id": 1})
        for doc in cursor:
            identifier = doc.get("_id")
            if identifier is None:
                continue
            text = str(identifier).strip()
            if text:
                codes.add(text)
    except Exception as exc:
        logger.warning("Unable to load kodSekolah for Sekolah Angkat Madani: %s", exc)

    logger.info("Loaded %d kodSekolah of Sekolah Angkat Madani", len(codes))
    return codes


def _replace_collection(
    collection: Collection,
    documents: Iterable[Dict[str, Any]],
    *,
    batch_size: int,
) -> dict[str, int]:
    if batch_size <= 0:
        raise ValueError("batch size must be positive")

    processed = 0
    inserted = 0
    updated = 0
    skipped = 0
    unchanged = 0

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

            incoming_checksum = document.get("checksum")
            comparable_fields = {
                key: value
                for key, value in document.items()
                if key not in COMPARISON_EXCLUDE_KEYS
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
                unchanged += 1
                continue

            logger.debug("Updating %s: changed fields = %s", identifier, list(changes.keys()),)

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

        if not operations:
            continue

        result = collection.bulk_write(operations, ordered=False)
        inserted += getattr(result, "upserted_count", 0) or 0
        updated += getattr(result, "modified_count", 0) or 0

    return {
        "processed": processed,
        "inserted": inserted,
        "updated": updated,
        "unchanged": unchanged,
        "skipped": skipped,
    }


def _mark_missing_schools_inactive(
    collection: Collection,
    present_identifiers: set[Any],
) -> int:
    selector = {"status": SekolahStatus.ACTIVE.value}
    if present_identifiers:
        selector["_id"] = {"$nin": list(present_identifiers)}

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
    logger.info("Starting ingestion")
    database = _get_database(settings)
    sekolah_collection = database[Sekolah.collection_name]
    entiti_collection = database[settings.entiti_sekolah_collection]
    kodSekolah_madani = _load_kodSekolah_madani(database, settings)
    documents, errors, total, present_identifiers = _collect_documents(settings, kodSekolah_madani)

    for document in documents:
        # All schools present in raw file are ACTIVE
        document["status"] = SekolahStatus.ACTIVE.value
        checksum = _compute_checksum(document)
        document["checksum"] = checksum

        identifier = document.get("_id") or document.get("kodSekolah")
        if identifier is None:
            continue

    try:
        result = _replace_collection(
            sekolah_collection,
            documents,
            batch_size=settings.batch_size,
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
        present_identifiers,
    )
    logger.info("Marked %s sekolah as inactive", inactivated)

    entiti_synced = sync_entiti_statuses(
        sekolah_collection,
        entiti_collection,
        batch_size=settings.batch_size,
    )
    logger.info("Synced %s EntitiSekolah statuses", entiti_synced)

    summary = {
        "collection": Sekolah.collection_name,
        "total": total,
        "processed": result["processed"],
        "failed": len(errors),
        "errors": errors,
        "inserted": result["inserted"],
        "updated": result["updated"],
        "unchanged": result["unchanged"],
        "skipped": result["skipped"],
        "inactivated": inactivated,
        "entiti_synced": entiti_synced,
    }
    return summary

def run_with_overrides(**overrides: Any) -> dict[str, Any]:
    settings = get_settings().model_copy(update=overrides)
    return run(settings)
