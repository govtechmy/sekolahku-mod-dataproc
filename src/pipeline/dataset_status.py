from __future__ import annotations

import logging

from pymongo import MongoClient
from pymongo.collection import Collection

from src.config.settings import Settings
from src.core.time import _utc_now

logger = logging.getLogger(__name__)


def _get_collection(settings: Settings) -> Collection:
    client = MongoClient(settings.mongo_uri)
    database = client[settings.db_name]
    return database[settings.dataset_status_collection]


def upsert_dataset_status(dataset_name: str, settings: Settings) -> None:
    """Upsert the lastUpdatedAt timestamp for a dataset into DatasetStatus.

    The document shape is:
    {"_id": <dataset_name>, "lastUpdatedAt": <UTC datetime>}
    """
    if not dataset_name:
        raise ValueError("dataset_name must be a non-empty string")

    try:
        collection = _get_collection(settings)
        collection.update_one(
            {"_id": dataset_name},
            {"$set": {"lastUpdatedAt": _utc_now()}},
            upsert=True,
        )
    except Exception:
        logger.exception("Failed to upsert DatasetStatus for dataset '%s'", dataset_name)
