"""Ingest Institusi (programme institutions) data from Google Sheet to MongoDB.

This mirrors the sekolah ingestion pipeline but targets the Institusi collection with
columns: NEGERI, PPD, JENIS/LABEL, KODINSTITUSI, NAMASEKOLAH, ENROLMEN PRA, GURU.
"""

from __future__ import annotations

import hashlib
import json
import logging
from typing import Any, Dict, Iterable, Iterator

import pandas as pd
from pydantic import ValidationError
from pymongo import MongoClient, UpdateOne
from pymongo.collection import Collection
from pymongo.errors import OperationFailure

from src.config import Settings, get_settings
from src.core.gsheet import fetch_csv_data
from src.core.s3 import _upload_to_s3, _read_csv_from_s3
from src.core.time import _utc_now
from src.models.institusi import Institusi
from src.models.sekolah import SekolahStatus

logger = logging.getLogger(__name__)

CHECKSUM_EXCLUDE_KEYS = {"_id", "createdAt", "updatedAt", "checksum", "status"}
COMPARISON_EXCLUDE_KEYS = {"_id", "createdAt", "updatedAt"}


def _compute_checksum(document: Dict[str, Any]) -> str:
    filtered = {key: document[key] for key in sorted(document) if key not in CHECKSUM_EXCLUDE_KEYS}
    serialized = json.dumps(filtered, sort_keys=True, separators=(",", ":"), ensure_ascii=False)
    return hashlib.sha256(serialized.encode("utf-8")).hexdigest()


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


def _load_rows(settings: Settings) -> Iterable[Dict[str, Any]]:
    if not settings.institusi_gsheet_id or not settings.institusi_gsheet_gid:
        raise RuntimeError("GSHEET_ID and INSTITUSI_GSHEET_GID must be set")

    csv_bytes = fetch_csv_data(settings.institusi_gsheet_id, settings.institusi_gsheet_gid)
    logger.info("Uploading Institusi CSV data to S3 bucket %s", settings.s3_bucket_dataproc)
    s3_key = _upload_to_s3(csv_bytes, settings.s3_bucket_dataproc, settings.s3_prefix_institusi)
    logger.info("Institusi CSV uploaded to S3 at key: %s", s3_key)

    df = _read_csv_from_s3(settings.s3_bucket_dataproc, s3_key)
    logger.info("Institusi CSV loaded from S3: %d rows, %d columns", df.shape[0], df.shape[1])
    return df.to_dict(orient="records")


def _collect_documents(settings: Settings) -> tuple[list[dict[str, Any]], list[dict[str, Any]], int, set[Any]]:
    documents: list[dict[str, Any]] = []
    errors: list[dict[str, Any]] = []
    total = 0
    present_identifiers: set[Any] = set()

    for index, row in enumerate(_load_rows(settings), start=1):
        total += 1

        raw_kod = str(row.get("KODINSTITUSI", "")).strip()
        if raw_kod:
            present_identifiers.add(raw_kod)

        try:
            institusi = Institusi.model_validate(row)
        except ValidationError as exc:
            messages_list = _format_validation_messages(exc)
            messages = "; ".join(messages_list)
            errors.append({"row": index, "error": messages})
            continue

        document = institusi.to_document()
        documents.append(document)

    return documents, errors, total, present_identifiers


def _replace_collection(collection: Collection, documents: Iterable[Dict[str, Any]], *, batch_size: int) -> dict[str, int]:
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
            identifier = document.get("_id") or document.get("kodInstitusi")
            if identifier is None:
                skipped += 1
                logger.warning("Skipping Institusi document without identifier: %s", document)
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

            comparable_fields = {key: value for key, value in document.items() if key not in COMPARISON_EXCLUDE_KEYS}

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

            logger.debug("Updating %s: changed fields = %s", identifier, list(changes.keys()))

            created_at_on_insert = (existing.get("createdAt") if existing else None) or document.get("createdAt") or _utc_now()

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


def _mark_missing_institusi_inactive(
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


def run_institusi(settings: Settings) -> dict[str, Any]:
    logger.info("Starting Institusi ingestion")
    database = _get_database(settings)
    institusi_collection = database[Institusi.collection_name]

    documents, errors, total, present_identifiers = _collect_documents(settings)

    for document in documents:
        # All institutions present in raw file are ACTIVE
        document["status"] = SekolahStatus.ACTIVE.value
        checksum = _compute_checksum(document)
        document["checksum"] = checksum

        identifier = document.get("_id") or document.get("kodInstitusi")
        if identifier is None:
            continue

    try:
        result = _replace_collection(
            institusi_collection,
            documents,
            batch_size=settings.batch_size,
        )
    except OperationFailure as exc:
        if exc.code == 13:
            logger.error("MongoDB authentication failed. Ensure your MONGO_URI includes the correct credentials and authSource if required.")
            raise RuntimeError("MongoDB authentication failed; check credentials and auth source") from exc
        logger.error("MongoDB operation failed: %s", exc)
        raise

    inactivated = _mark_missing_institusi_inactive(
        institusi_collection,
        present_identifiers,
    )
    logger.info("Marked %s Institusi as inactive", inactivated)

    summary = {
        "collection": Institusi.collection_name,
        "total": total,
        "processed": result["processed"],
        "failed": len(errors),
        "errors": errors,
        "inserted": result["inserted"],
        "updated": result["updated"],
        "skipped": result["skipped"],
        "inactivated": inactivated,
    }
    logger.info(
        "Institusi ingestion completed: processed=%s, inserted=%s, updated=%s, failed=%s, inactivated=%s",
        result["processed"],
        result["inserted"],
        result["updated"],
        len(errors),
        inactivated,
    )
    return summary


def run_with_overrides(**overrides: Any) -> dict[str, Any]:
    settings = get_settings().model_copy(update=overrides)
    return run(settings)


__all__ = ["run", "run_with_overrides"]
