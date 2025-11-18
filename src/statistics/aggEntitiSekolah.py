from __future__ import annotations

import logging
from typing import Any, Dict, List

from pydantic import ValidationError
from pymongo.collection import Collection

from src.models import Sekolah
from src.models.entitiSekolah import EntitiSekolah

logger = logging.getLogger(__name__)

ENTITI_SEKOLAH_COLLECTION = EntitiSekolah.collection_name


def _build_entiti_document(raw: Dict[str, Any]) -> Dict[str, Any]:
    try:
        sekolah = Sekolah.model_validate(raw)
    except ValidationError as exc:  # pragma: no cover - defensive logging
        logger.warning("Skipping sekolah document due to validation error: %s", exc)
        return {}

    tahun_penubuhan = raw.get("tahunPenubuhan")
    entiti = EntitiSekolah.from_sekolah(sekolah, tahun_penubuhan=tahun_penubuhan)
    document = entiti.to_document()

    # Preserve the original identifier for traceability if present.
    if "_id" in raw:
        document["_id"] = raw["_id"]
    return document


def compute_entiti_sekolah(collection: Collection) -> List[Dict[str, Any]]:
    """Project sekolah collection into the EntitiSekolah aggregation view."""

    documents: List[Dict[str, Any]] = []
    for raw in collection.find({}):
        document = _build_entiti_document(raw)
        if document:
            documents.append(document)
    return documents


__all__ = ["compute_entiti_sekolah", "ENTITI_SEKOLAH_COLLECTION"]
