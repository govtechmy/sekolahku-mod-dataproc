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
    collection: str


def _get_db(settings: Settings) -> Database:
    client = MongoClient(settings.mongo_uri)
    return client[settings.db_name]

def _persist_analitik(collection: Collection, document: dict) -> dict:
    """Overwrite data for every run."""
    collection.replace_one({"_id": "1"}, document, upsert=True)

    return {
        "collection": collection.name,
        "processed": 1,
        "inserted": 1,
        "updated": 0,
        "skipped": 0,
    }


def run_analitik_sekolah(settings: Optional[Settings] = None) -> PersistAnalitikResult:
    """Generate the AnalitikSekolah aggregation output."""

    settings = settings or get_settings()
    db = _get_db(settings)
    sekolah_collection = db[Sekolah.collection_name]
    institusi_collection = db[settings.institusi_collection]
    analitik_collection = db[ANALITIK_SEKOLAH_COLLECTION]

    documents = compute_analitik_sekolah(sekolah_collection, institusi_collection)
    if not documents:
        return {"collection": ANALITIK_SEKOLAH_COLLECTION, "processed": 0, "inserted": 0, "updated": 0, "skipped": 0}

    # Only one document
    doc = documents[0]

    result = _persist_analitik(analitik_collection, doc)
    return result


def run_analitik_dict(settings: Optional[Settings] = None) -> Dict[str, PersistAnalitikResult]:
    """Convenience helper returning pipeline output as a serialisable dict."""

    return {"analitik": run_analitik_sekolah(settings)}


__all__ = ["run_analitik_sekolah", "run_analitik_dict"]