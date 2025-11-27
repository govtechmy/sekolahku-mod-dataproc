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
from src.pipeline.ingestion import _replace_collection

logger = logging.getLogger(__name__)


class PersistAnalitikResult(TypedDict):
    processed: int
    inserted: int
    updated: int
    skipped: int
    dry_run: bool
    collection: str


def _get_db(settings: Settings) -> Database:
    client = MongoClient(settings.mongo_uri)
    return client[settings.db_name]


def _persist_analitik(
    collection: Collection,
    documents: list[dict],
    *,
    batch_size: int,
    dry_run: bool,
) -> PersistAnalitikResult:
    if not documents:
        logger.info("No analytics documents to persist to collection %s", collection.name)
        outcome = {"processed": 0, "inserted": 0, "updated": 0, "skipped": 0, "dry_run": dry_run}
    else:
        outcome = _replace_collection(
            collection,
            documents,
            batch_size=batch_size,
            dry_run=dry_run,
        )

    return {
        "collection": collection.name,
        "processed": outcome["processed"],
        "inserted": outcome["inserted"],
        "updated": outcome["updated"],
        "skipped": outcome["skipped"],
        "dry_run": outcome["dry_run"],
    }


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

    result = _persist_analitik(
        analitik_collection,
        documents,
        batch_size=settings.batch_size,
        dry_run=settings.dry_run,
    )
    return result


def run_analitik_dict(settings: Optional[Settings] = None) -> Dict[str, PersistAnalitikResult]:
    """Convenience helper returning pipeline output as a serialisable dict."""

    return {"analitik": run_analitik_sekolah(settings)}


__all__ = ["run_analitik_sekolah", "run_analitik_dict"]