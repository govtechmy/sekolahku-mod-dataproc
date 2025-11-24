"""Pipeline for generating the EntitiSekolah aggregation view."""
from __future__ import annotations

import logging
from typing import Any, Dict

from pymongo import MongoClient
from pymongo.collection import Collection
from pymongo.database import Database

from src.config.settings import Settings, get_settings
from src.models import Sekolah
from src.statistics import ENTITI_SEKOLAH_COLLECTION, compute_entiti_sekolah

logger = logging.getLogger(__name__)


def _get_db(settings: Settings) -> Database:
    client = MongoClient(settings.mongo_uri)
    return client[settings.db_name]


def _persist_entiti(collection: Collection, documents: list[dict], dry_run: bool) -> Dict[str, int | bool]:
    processed = len(documents)
    inserted = 0

    if dry_run:
        logger.info("Dry run enabled; skipping write to collection %s", collection.name)
        return {"processed": processed, "inserted": inserted, "dry_run": True}

    collection.delete_many({})
    if documents:
        collection.insert_many(documents, ordered=False)
        inserted = len(documents)

    return {"processed": processed, "inserted": inserted, "dry_run": False}


def run_entiti_sekolah(settings: Settings | None = None) -> Dict[str, Any]:
    """Generate and persist the EntitiSekolah aggregation output."""

    settings = settings or get_settings()
    db = _get_db(settings)
    sekolah_collection = db[Sekolah.collection_name]
    entiti_collection = db[ENTITI_SEKOLAH_COLLECTION]

    documents = compute_entiti_sekolah(sekolah_collection)
    logger.info(
        "Computed %s EntitiSekolah documents from collection %s",
        len(documents),
        Sekolah.collection_name,
    )

    result = _persist_entiti(entiti_collection, documents, settings.dry_run)
    summary = {
        "collection": ENTITI_SEKOLAH_COLLECTION,
        "total": result["processed"],
        "processed": result["processed"],
        "failed": 0,
        "errors": [],
        "inserted": result["inserted"],
        "dry_run": result["dry_run"],
    }
    logger.info("Entiti summary: %s", summary)
    return summary


def run_entiti_sekolah_dict(settings: Settings | None = None) -> Dict[str, Dict[str, Any]]:
    """Convenience helper returning pipeline output as a serialisable dict."""

    return {"entiti": run_entiti_sekolah(settings)}


__all__ = ["run_entiti_sekolah", "run_entiti_sekolah_dict"]
