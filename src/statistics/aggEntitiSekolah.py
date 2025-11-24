from __future__ import annotations

import logging
from typing import Any, Dict, List, Optional

from pydantic import ValidationError
from pymongo.collection import Collection

from src.models import Sekolah
from src.models.entitiSekolah import EntitiSekolah, SekolahBerdekatan, SekolahBerdekatanItem

logger = logging.getLogger(__name__)

ENTITI_SEKOLAH_COLLECTION = EntitiSekolah.collection_name


_MAX_NEARBY = 5
_NEARBY_PROJECTION = {
    "_id": 0,
    "kodSekolah": 1,
    "namaSekolah": 1,
    "bandarSurat": 1,
    "negeri": 1,
    "dun": 1,
    "ppd": 1,
    "parlimen": 1,
}


def _normalize_upper(value: Optional[str]) -> Optional[str]:
    if value is None:
        return None
    text = str(value).strip().upper()
    return text or None


def _clean_string(value: Optional[str]) -> Optional[str]:
    if value is None:
        return None
    text = str(value).strip()
    return text or None


def _build_nearby_schools(
    collection: Collection,
    sekolah: Sekolah,
) -> SekolahBerdekatan:
    negeri = _normalize_upper(sekolah.negeri)
    if negeri is None:
        return SekolahBerdekatan()

    bandar = _normalize_upper(sekolah.bandarSurat)
    dun = _normalize_upper(sekolah.dun)
    ppd = _normalize_upper(sekolah.ppd)
    parlimen = _normalize_upper(sekolah.parlimen)

    results: list[SekolahBerdekatanItem] = []
    seen: set[str] = set()

    def add_candidates(extra_filter: Dict[str, Any]) -> None:
        remaining = _MAX_NEARBY - len(results)
        if remaining <= 0:
            return

        query: Dict[str, Any] = {
            "negeri": negeri,
            **extra_filter,
            "kodSekolah": {"$ne": sekolah.kodSekolah},
        }

        cursor = collection.find(query, projection=_NEARBY_PROJECTION).limit(remaining)
        try:
            for doc in cursor:
                raw_code = doc.get("kodSekolah")
                code = _normalize_upper(raw_code)
                if not code or code in seen:
                    continue

                results.append(
                    SekolahBerdekatanItem(
                        namaSekolah=_clean_string(doc.get("namaSekolah")),
                        kodSekolah=code,
                        bandarSurat=_clean_string(doc.get("bandarSurat")),
                        negeri=_clean_string(doc.get("negeri")),
                    )
                )
                seen.add(code)

                if len(results) >= _MAX_NEARBY:
                    break
        finally:
            cursor.close()

    if bandar is not None:
        add_candidates({"bandarSurat": bandar})

    if len(results) < _MAX_NEARBY and dun is not None:
        add_candidates({"dun": dun})

    if len(results) < _MAX_NEARBY and parlimen is not None:
        add_candidates({"parlimen": parlimen})

    if len(results) < _MAX_NEARBY and ppd is not None:
        add_candidates({"ppd": ppd})

    if len(results) < _MAX_NEARBY:
        add_candidates({})

    return SekolahBerdekatan(senarai=results[:_MAX_NEARBY])


def _build_entiti_document(
    raw: Dict[str, Any],
    sekolah: Sekolah,
    nearby: SekolahBerdekatan,
) -> Dict[str, Any]:
    entiti = EntitiSekolah.from_sekolah(
        sekolah,
        sekolah_berdekatan=nearby,
    )
    document = entiti.to_document()

    # Preserve the original identifier for traceability if present.
    if "_id" in raw:
        document["_id"] = raw["_id"]
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
        nearby = _build_nearby_schools(collection, sekolah)
        document = _build_entiti_document(raw, sekolah, nearby)
        documents.append(document)
    return documents


__all__ = ["compute_entiti_sekolah", "ENTITI_SEKOLAH_COLLECTION"]
