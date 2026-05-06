from __future__ import annotations

from typing import Any

from pymongo import UpdateOne
from pymongo.collection import Collection

from src.models.sekolah import SekolahStatus


def _normalize_status(value: Any) -> Any:
    if isinstance(value, SekolahStatus):
        return value.value
    return value


def delete_inactive_entiti(entiti_collection: Collection) -> int:
    """Delete INACTIVE records from EntitiSekolah collection."""
    result = entiti_collection.delete_many({"status": SekolahStatus.INACTIVE.value})
    return result.deleted_count


def sync_entiti_statuses(
    sekolah_collection: Collection,
    entiti_collection: Collection,
    *,
    batch_size: int,
) -> int:
    if batch_size <= 0:
        raise ValueError("batch size must be positive")

    total_synced = 0
    identifiers: list[Any] = []
    status_map: dict[Any, Any] = {}

    cursor = sekolah_collection.find({}, {"_id": 1, "status": 1})

    def flush() -> int:
        if not identifiers:
            return 0

        existing_cursor = entiti_collection.find({"_id": {"$in": identifiers}}, {"_id": 1, "status": 1})
        entiti_status_map = {doc["_id"]: _normalize_status(doc.get("status")) for doc in existing_cursor}

        operations: list[UpdateOne] = []
        for identifier in identifiers:
            sekolah_status = _normalize_status(status_map[identifier])
            entiti_status = entiti_status_map.get(identifier)
            if entiti_status == sekolah_status:
                continue

            update_document = {
                "$set": {"status": sekolah_status},
                "$currentDate": {"updatedAt": True},
            }
            operations.append(UpdateOne({"_id": identifier}, update_document))

        if not operations:
            return 0

        result = entiti_collection.bulk_write(operations, ordered=False)
        modified = getattr(result, "modified_count", 0) or 0
        upserted = getattr(result, "upserted_count", 0) or 0
        return int(modified + upserted)

    for record in cursor:
        identifier = record.get("_id")
        if identifier is None:
            continue
        identifiers.append(identifier)
        status_map[identifier] = record.get("status")

        if len(identifiers) >= batch_size:
            total_synced += flush()
            identifiers.clear()
            status_map.clear()

    if identifiers:
        total_synced += flush()

    return total_synced


__all__ = ["sync_entiti_statuses", "delete_inactive_entiti"]
