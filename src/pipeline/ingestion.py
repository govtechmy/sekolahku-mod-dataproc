from __future__ import annotations

import csv
import hashlib
import json
import logging
import os
from datetime import datetime, timezone
from typing import Any, Dict, Iterable, Iterator

from pydantic import ValidationError
from pymongo import MongoClient, UpdateOne
from pymongo.collection import Collection
from pymongo.errors import OperationFailure

from src.config import Settings, get_settings
from src.models import Sekolah
from src.models.sekolah import SekolahStatus
from src.pipeline.status_sync import sync_entiti_statuses


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


def _read_csv(path: str) -> Iterable[Dict[str, Any]]:
    if not os.path.exists(path):
        # raise FileNotFoundError(f"CSV not found: {path}")
        logging.warning(f"CSV not found at {path}. Skipping ingestion.")
        return []
    with open(path, newline="", encoding="utf-8") as handle:
        yield from csv.DictReader(handle)


def _load_rows(settings: Settings) -> Iterable[Dict[str, Any]]:
    logger.info("Loading data from CSV: %s", settings.csv_path)
    return _read_csv(settings.csv_path)


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

            incoming_checksum = document.get("checksum")
            if (
                existing is not None
                and incoming_checksum is not None
                and existing.get("checksum") == incoming_checksum
            ):
                continue

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
        "skipped": skipped,
    }


def _mark_missing_schools_inactive(
    collection: Collection,
    active_identifiers: set[Any],
) -> int:
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
    logger.info("Starting ingestion")
    database = _get_database(settings)
    sekolah_collection = database[Sekolah.collection_name]
    entiti_collection = database[settings.entiti_sekolah_collection]
    documents, errors, total = _collect_documents(settings)

    active_identifiers: set[Any] = set()
    for document in documents:
        document["status"] = SekolahStatus.ACTIVE.value  # All schools present in raw file are ACTIVE
        checksum = _compute_checksum(document)
        document["checksum"] = checksum

        identifier = document.get("_id") or document.get("kodSekolah")
        if identifier is None:
            continue
        # All schools present in raw file are ACTIVE
        document["status"] = SekolahStatus.ACTIVE.value
        active_identifiers.add(identifier)

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
        active_identifiers,
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
        "skipped": result["skipped"],
        "inactivated": inactivated,
        "entiti_synced": entiti_synced,
    }
    return summary


def run_with_overrides(**overrides: Any) -> dict[str, Any]:
    settings = get_settings().model_copy(update=overrides)
    return run(settings)
