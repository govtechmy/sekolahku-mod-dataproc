"""Statistics pipeline runner.

Provides a convenience function to compute aggregated statistics
for schools, teachers, and students using the existing Mongo connection.
"""
from __future__ import annotations

from pymongo import MongoClient
from pymongo.database import Database
from src.config.settings import Settings, get_settings
from src.models import Sekolah
from src.models.statistics import StatistikSummary, StatistikSekolah, StatistikGuru, StatistikMurid
from src.statistics.aggregations import (
    STATISTIK_GURU_COLLECTION,
    STATISTIK_MURID_COLLECTION,
    STATISTIK_SEKOLAH_COLLECTION,
    build_statistik_documents,
    compute_all_statistics,
)


def _get_db(settings: Settings) -> Database:
    client = MongoClient(settings.mongo_uri)
    return client[settings.db_name]


def _replace_collection(db: Database, collection_name: str, document: dict) -> int:
    collection = db[collection_name]
    collection.delete_many({})
    if document:
        collection.insert_one(document)
        return 1
    return 0


def _persist_statistics_collections(db: Database, stats: dict[str, dict]) -> dict[str, int]:
    documents = build_statistik_documents(stats)
    return {
        STATISTIK_SEKOLAH_COLLECTION: _replace_collection(db, STATISTIK_SEKOLAH_COLLECTION, documents[STATISTIK_SEKOLAH_COLLECTION]),
        STATISTIK_GURU_COLLECTION: _replace_collection(db, STATISTIK_GURU_COLLECTION, documents[STATISTIK_GURU_COLLECTION]),
        STATISTIK_MURID_COLLECTION: _replace_collection(db, STATISTIK_MURID_COLLECTION, documents[STATISTIK_MURID_COLLECTION]),
    }


def run_statistics(settings: Settings | None = None) -> StatistikSummary:
    settings = settings or get_settings()
    db = _get_db(settings)
    collection = db[Sekolah.collection_name]
    raw = compute_all_statistics(collection)
    _persist_statistics_collections(db, raw)
    return StatistikSummary(
        sekolah=StatistikSekolah(**raw["sekolah"]["data"]),
        guru=StatistikGuru(**raw["guru"]["data"]),
        murid=StatistikMurid(**raw["murid"]["data"]),
    )


def run_statistics_dict(settings: Settings | None = None) -> dict:
    return run_statistics(settings).as_dict()
