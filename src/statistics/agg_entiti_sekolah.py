from __future__ import annotations

import logging
from typing import Any, Dict, List

from pydantic import ValidationError
from pymongo.collection import Collection

from src.models import Sekolah
from src.models.entiti_sekolah import EntitiSekolah

logger = logging.getLogger(__name__)

ENTITI_SEKOLAH_COLLECTION = EntitiSekolah.collection_name


def _build_entiti_document(
    raw: Dict[str, Any],
    sekolah: Sekolah,
) -> Dict[str, Any]:
    entiti = EntitiSekolah.from_sekolah(sekolah)
    document = entiti.to_document()
    document["_id"] = sekolah.kodSekolah
    return document


def compute_entiti_sekolah(collection: Collection) -> List[Dict[str, Any]]:
    """Project sekolah collection into the EntitiSekolah aggregation view."""

    documents: List[Dict[str, Any]] = []
    for raw in collection.find({}):
        try:
            sekolah = Sekolah.model_validate(raw)
        except ValidationError as exc:  # pragma: no cover - defensive logging
            logger.warning("Skipping sekolah document due to validation error: %s", exc)
            continue
        document = _build_entiti_document(raw, sekolah)
        documents.append(document)
    return documents


__all__ = ["compute_entiti_sekolah", "ENTITI_SEKOLAH_COLLECTION"]
