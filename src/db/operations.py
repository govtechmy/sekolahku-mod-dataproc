"""Database write operations for Mongo collections."""
from __future__ import annotations

from typing import Iterable, Dict, Any, Iterator

from pymongo.collection import Collection


def chunked(iterable: Iterable[Dict[str, Any]], size: int) -> Iterator[list[Dict[str, Any]]]:
    """Yield fixed-size chunks from an iterable."""
    if size <= 0:
        raise ValueError("chunk size must be positive")
    batch: list[Dict[str, Any]] = []
    for item in iterable:
        batch.append(item)
        if len(batch) >= size:
            yield batch
            batch = []
    if batch:
        yield batch


def replace_documents(
    collection: Collection,
    documents: Iterable[Dict[str, Any]],
    *,
    batch_size: int = 500,
    dry_run: bool = False,
) -> dict[str, int]:
    """Replace all documents in the collection with the provided iterable."""
    if batch_size <= 0:
        raise ValueError("batch_size must be positive")

    inserted = 0
    if not dry_run:
        collection.delete_many({})

    for batch in chunked(documents, batch_size):
        inserted += len(batch)
        if dry_run or not batch:
            continue
        collection.insert_many(batch, ordered=False)

    return {"inserted": inserted, "dry_run": int(dry_run)}
