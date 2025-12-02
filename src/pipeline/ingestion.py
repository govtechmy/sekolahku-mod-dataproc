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
# COMPARISON_EXCLUDE_KEYS = {"_id", "createdAt", "updatedAt"}
COMPARISON_EXCLUDE_KEYS = {"_id", "createdAt", "updatedAt", "checksum"}


def _compute_checksum(document: Dict[str, Any]) -> str:
    filtered = {
        key: document[key]
        for key in sorted(document)
        if key not in CHECKSUM_EXCLUDE_KEYS
    }
    serialized = json.dumps(filtered, sort_keys=True, separators=(",", ":"), ensure_ascii=False)
    return hashlib.sha256(serialized.encode("utf-8")).hexdigest()


def _utc_now() -> datetime:
    return datetime.now(timezone.utc)

logger = logging.getLogger(__name__)


def _read_csv(path: str) -> Iterable[Dict[str, Any]]:
    if not os.path.exists(path):
        logger.warning("CSV not found at %s. Skipping ingestion.", path)
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
            message = message[len(prefix) :]
        messages.append(message)
    if not messages:
        messages.append(str(exc))
    return messages


def _collect_documents(
    settings: Settings,
) -> tuple[list[dict[str, Any]], list[dict[str, Any]], int, set[str]]:
    """Load rows, validate into Sekolah documents, and collect errors.

    Returns (documents, errors, total_rows, active_ids_from_csv).
    active_ids_from_csv is the set of all kodSekolah values seen in the CSV,
    regardless of whether validation succeeded.
    """
    documents: list[dict[str, Any]] = []
    errors: list[dict[str, Any]] = []
    total = 0
    active_ids_from_csv: set[str] = set()

    for index, row in enumerate(_load_rows(settings), start=1):
        total += 1
        raw_id = (row.get("KODSEKOLAH") or "").strip()
        if raw_id:
            active_ids_from_csv.add(raw_id)

        try:
            sekolah = Sekolah.model_validate(row)
        except ValidationError as exc:  # pragma: no cover - logging aid
            messages_list = _format_validation_messages(exc)
            messages = "; ".join(messages_list)
            errors.append({"row": index, "error": messages})
            continue

        documents.append(sekolah.to_document())

    return documents, errors, total, active_ids_from_csv


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
                # Ensure status is ACTIVE for current-run schools
                operations.append(
                    UpdateOne(
                        {"_id": identifier},
                        {
                            "$set": {"status": SekolahStatus.ACTIVE.value},
                            "$currentDate": {"updatedAt": True},
                        },
                    )
                )
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

            # Always enforce status=ACTIVE for documents
            required_status = SekolahStatus.ACTIVE.value
            current_status = existing.get("status")

            if current_status != required_status:
                changes["status"] = required_status


            if existing is not None and not changes:
                continue

            logger.debug("Updating %s: changed fields = %s", identifier, list(changes.keys()))

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
    # Warn if there are still documents without a status field
    missing_status_count = collection.count_documents({"status": {"$exists": False}})
    if missing_status_count:
        logger.warning("Found %s documents without 'status' field", missing_status_count,)

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
    documents, errors, total, active_ids_from_csv = _collect_documents(settings)

    # 1. For all valid documents, compute checksum (status handled in _replace_collection).
    for document in documents:
        checksum = _compute_checksum(document)
        document["checksum"] = checksum

    # 2. Upsert valid documents and enforce status=ACTIVE for present schools.
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

    # 3. Compute ACTIVE/INACTIVE purely from set difference of _id vs kodSekolah.
    #    All _id values not present in active_ids_from_csv become INACTIVE.
    active_identifiers: set[Any] = set(active_ids_from_csv)

    inactivated = _mark_missing_schools_inactive(
        sekolah_collection,
        active_identifiers,
    )
    logger.info("Newly inactive schools found: %s", inactivated)

    entiti_synced = sync_entiti_statuses(
        sekolah_collection,
        entiti_collection,
        batch_size=settings.batch_size,
    )

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
