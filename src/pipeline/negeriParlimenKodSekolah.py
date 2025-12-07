from __future__ import annotations

import logging
from collections import defaultdict
from typing import Any, Dict, List, Tuple

from pymongo import MongoClient, UpdateOne
from pymongo.collection import Collection

from src.config.settings import Settings, get_settings
from src.models.negeriParlimenKodSekolah import NegeriParlimenKodSekolah
from src.models.sekolah import Sekolah
from src.pipeline.ingestion import _chunked


logger = logging.getLogger(__name__)


def _get_db(settings: Settings):
    client = MongoClient(settings.mongo_uri)
    return client[settings.db_name]


def _aggregate_negeri_parlimen_kod_sekolah(sekolah_collection: Collection) -> List[NegeriParlimenKodSekolah]:
    """Aggregate Sekolah documents into (negeri, parlimen) -> [kodSekolah] mapping."""

    mapping: dict[tuple[str, str], set[str]] = defaultdict(set)

    cursor = sekolah_collection.find(
        {},
        {"_id": 0, "negeri": 1, "parlimen": 1, "kodSekolah": 1},
    )

    for doc in cursor:
        negeri_raw = doc.get("negeri")
        parlimen_raw = doc.get("parlimen")
        kod = doc.get("kodSekolah")

        # kodSekolah must exist; everything else can be missing
        if kod is None:
            continue

        model = NegeriParlimenKodSekolah(
            negeri=negeri_raw,
            parlimen=parlimen_raw,
            kodSekolahList=[kod],
        )

        # Normalize missing negeri → UNKNOWN
        negeri_key = model.negeri.value if model.negeri else "UNKNOWN"

        # Normalize missing parlimen → UNKNOWN
        parlimen_key = model.parlimen if model.parlimen else "UNKNOWN"

        key = (negeri_key, parlimen_key)
        mapping[key].add(kod)

    results: List[NegeriParlimenKodSekolah] = []
    for (negeri_key, parlimen_key), codes in mapping.items():
        results.append(
            NegeriParlimenKodSekolah(
                negeri=None if negeri_key == "UNKNOWN" else negeri_key,
                parlimen=None if parlimen_key == "UNKNOWN" else parlimen_key,
                kodSekolahList=sorted(codes),
            )
        )

    return results



def _upsert_documents(collection: Collection, models: List[NegeriParlimenKodSekolah], *, batch_size: int) -> Dict[str, int]:
    processed = 0
    inserted = 0
    updated = 0
    skipped = 0

    for chunk in _chunked(models, batch_size):
        operations: list[UpdateOne] = []

        for model in chunk:
            processed += 1
            doc = model.to_document()
            _id = doc.get("_id")
            if _id is None:
                skipped += 1
                continue

            update_doc = {"$set": {"negeri": doc["negeri"], "parlimen": doc["parlimen"], "kodSekolahList": doc["kodSekolahList"]}}
            operations.append(UpdateOne({"_id": _id}, update_doc, upsert=True))

        if not operations:
            continue

        result = collection.bulk_write(operations, ordered=False)
        inserted += getattr(result, "upserted_count", 0) or 0
        updated += getattr(result, "modified_count", 0) or 0

    return {"processed": processed, "inserted": inserted, "updated": updated, "skipped": skipped}


def run_negeri_parlimen_kod_sekolah(settings: Settings | None = None) -> Dict[str, Any]:
    """Populate NegeriParlimenKodSekolah collection from Sekolah documents in MongoDB."""

    settings = settings or get_settings()
    db = _get_db(settings)
    sekolah_collection = db[Sekolah.collection_name]
    target_collection = db[NegeriParlimenKodSekolah.collection_name]

    models = _aggregate_negeri_parlimen_kod_sekolah(sekolah_collection)

    if not models:
        logger.info("No negeri-parlimen-kodSekolah mappings derived from Sekolah collection")
        return {
            "collection": NegeriParlimenKodSekolah.collection_name,
            "processed": 0,
            "inserted": 0,
            "updated": 0,
            "skipped": 0,
        }

    result = _upsert_documents(target_collection, models, batch_size=settings.batch_size)
    summary: Dict[str, Any] = {"collection": NegeriParlimenKodSekolah.collection_name, **result}
    return summary


__all__ = ["run_negeri_parlimen_kod_sekolah"]
