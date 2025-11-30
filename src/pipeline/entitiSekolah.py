"""Pipeline for generating the EntitiSekolah aggregation view."""
from __future__ import annotations

import logging
from typing import Any, Dict, TypedDict

from pymongo import MongoClient
from pymongo.collection import Collection
from pymongo.database import Database

from src.config.settings import Settings, get_settings
from src.models import Sekolah
from src.statistics import ENTITI_SEKOLAH_COLLECTION, compute_entiti_sekolah
from src.pipeline.ingestion import _replace_collection

logger = logging.getLogger(__name__)


class PersistEntitiResult(TypedDict):
    processed: int
    inserted: int
    updated: int
    skipped: int


def _get_db(settings: Settings) -> Database:
    client = MongoClient(settings.mongo_uri)
    return client[settings.db_name]


def _persist_entiti(
    collection: Collection,
    documents: list[dict],
    *,
    batch_size: int,
) -> PersistEntitiResult:
    if not documents:
        logger.info("No entiti documents to persist to collection %s", collection.name)
        outcome = {"processed": 0, "inserted": 0, "updated": 0, "skipped": 0}
    else:
        # Ensure identifiers are populated for upsert comparison.
        for document in documents:
            if "_id" not in document and document.get("kodSekolah"):
                document["_id"] = document["kodSekolah"]

        outcome = _replace_collection(
            collection,
            documents,
            batch_size=batch_size,
        )

    return {
        "processed": outcome["processed"],
        "inserted": outcome["inserted"],
        "updated": outcome["updated"],
        "skipped": outcome["skipped"],
    }


def run_entiti_sekolah(settings: Settings | None = None) -> Dict[str, Any]:
    """Generate and persist the EntitiSekolah aggregation output."""

    settings = settings or get_settings()
    db = _get_db(settings)
    db_name = db[Sekolah.collection_name]
    entiti_collection = db[ENTITI_SEKOLAH_COLLECTION]

    documents = compute_entiti_sekolah(db_name)

    result = _persist_entiti(
        entiti_collection,
        documents,
        batch_size=settings.batch_size,
    )
    summary = {
        "collection": ENTITI_SEKOLAH_COLLECTION,
        "total": result["processed"],
        "processed": result["processed"],
        "failed": 0,
        "errors": [],
        "inserted": result["inserted"],
        "updated": result["updated"],
        "skipped": result["skipped"],
    }
    logger.info("Entiti summary: %s", summary)
    return summary


def run_entiti_sekolah_dict(settings: Settings | None = None) -> Dict[str, Dict[str, Any]]:
    """Convenience helper returning pipeline output as a serialisable dict."""

    return {"entiti": run_entiti_sekolah(settings)}


__all__ = ["run_entiti_sekolah", "run_entiti_sekolah_dict"]
