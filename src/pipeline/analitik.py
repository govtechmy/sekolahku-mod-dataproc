"""Pipeline for generating the AnalitikSekolah aggregation view."""
from __future__ import annotations

import logging
from typing import Dict, Optional, TypedDict

from pymongo import MongoClient
from pymongo.collection import Collection
from pymongo.database import Database

from src.config.settings import Settings, get_settings
from src.models import Sekolah
from src.statistics import ANALITIK_SEKOLAH_COLLECTION, compute_analitik_sekolah

logger = logging.getLogger(__name__)


class PersistAnalitikResult(TypedDict):
    processed: int
    inserted: int
    dry_run: bool
    collection: str


def _get_db(settings: Settings) -> Database:
    client = MongoClient(settings.mongo_uri)
    return client[settings.db_name]


def _persist_analitik(collection: Collection, documents: list[dict], dry_run: bool) -> PersistAnalitikResult:
    processed = len(documents)
    inserted = 0

    if dry_run:
        logger.info("Dry run enabled; skipping write to collection %s", collection.name)
        return {"processed": processed, "inserted": inserted, "dry_run": True, "collection": collection.name}

    collection.delete_many({})
    if documents:
        collection.insert_many(documents, ordered=False)
        inserted = len(documents)

    return {"processed": processed, "inserted": inserted, "dry_run": False, "collection": collection.name}


def run_analitik_sekolah(settings: Optional[Settings] = None) -> PersistAnalitikResult:
    """Generate and persist the AnalitikSekolah aggregation output."""

    settings = settings or get_settings()
    db = _get_db(settings)
    db_name = db[Sekolah.collection_name]
    analitik_collection = db[ANALITIK_SEKOLAH_COLLECTION]

    documents = compute_analitik_sekolah(db_name)
    logger.info(
        "Computed %s AnalitikSekolah documents from collection %s",
        len(documents),
        Sekolah.collection_name,
    )

    result = _persist_analitik(analitik_collection, documents, settings.dry_run)
    return result


def run_analitik_dict(settings: Optional[Settings] = None) -> Dict[str, PersistAnalitikResult]:
    """Convenience helper returning pipeline output as a serialisable dict."""

    return {"analitik": run_analitik_sekolah(settings)}


__all__ = ["run_analitik_sekolah", "run_analitik_dict"]