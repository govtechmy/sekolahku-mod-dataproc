from __future__ import annotations

import logging

from src.config.settings import Settings
from src.core.time import _utc_now
from src.utils.db.get_db_collection import get_db_collection
from typing import Optional

logger = logging.getLogger(__name__)


def upsert_dataset_status(dataset_name: str, settings: Settings, file_version: Optional[str]) -> None:
    """Upsert the lastUpdatedAt timestamp for a dataset into DatasetStatus.

    The document shape is:
    {"_id": <dataset_name>, "lastUpdatedAt": <UTC datetime>, "fileVersion": <file_version>}
    fileVersion is only set if provided (not None).
    """
    if not dataset_name:
        raise ValueError("dataset_name must be a non-empty string")

    try:
        collection = get_db_collection(settings, name=settings.dataset_status_collection)
        update_fields = {"lastUpdatedAt": _utc_now()}
        if file_version is not None:
            update_fields["fileVersion"] = file_version
        collection.update_one(
            {"_id": dataset_name},
            {"$set": update_fields},
            upsert=True,
        )
    except Exception:
        logger.exception("Failed to upsert DatasetStatus for dataset '%s'", dataset_name)
