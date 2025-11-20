from __future__ import annotations

import logging
from typing import Any, Dict, Iterable, List, Optional

from pydantic import ValidationError
from pymongo.collection import Collection

from src.models import Sekolah
from src.models.entitiSekolah import EntitiSekolah, SekolahBerdekatan, SekolahBerdekatanItem

logger = logging.getLogger(__name__)

ENTITI_SEKOLAH_COLLECTION = EntitiSekolah.collection_name


def _build_nearby_schools(
    sekolah: Sekolah,
    all_schools: Iterable[Sekolah],
) -> SekolahBerdekatan:
    def _normalize(value: Optional[str]) -> Optional[str]:
        if value is None:
            return None
        text = str(value).strip().upper()
        return text or None

    target_negeri = _normalize(sekolah.negeri)
    target_bandar = _normalize(sekolah.bandarSurat)
    target_dun = _normalize(sekolah.dun)
    target_ppd = _normalize(sekolah.ppd)

    candidates: list[tuple[Sekolah, dict[str, Optional[str]]]] = []
    for other in all_schools:
        if other.kodSekolah == sekolah.kodSekolah:
            continue
        candidates.append(
            (
                other,
                {
                    "negeri": _normalize(other.negeri),
                    "bandarSurat": _normalize(other.bandarSurat),
                    "dun": _normalize(other.dun),
                    "ppd": _normalize(other.ppd),
                },
            )
        )

    selected: list[Sekolah] = []
    seen: set[str] = set()

    def add_matches(*, require_negeri: bool, match_field: Optional[str]) -> None:
        if len(selected) >= 5:
            return
        target_value = None
        if match_field:
            target_value = {
                "bandarSurat": target_bandar,
                "dun": target_dun,
                "ppd": target_ppd,
            }[match_field]
            if target_value is None:
                return

        for other, attrs in candidates:
            if len(selected) >= 5:
                break
            if other.kodSekolah in seen:
                continue

            if require_negeri:
                if target_negeri is None:
                    continue
                if attrs["negeri"] != target_negeri:
                    continue

            if match_field:
                if attrs[match_field] != target_value:
                    continue

            selected.append(other)
            seen.add(other.kodSekolah)

    # Step 1: same negeri + bandarSurat
    add_matches(require_negeri=True, match_field="bandarSurat")

    # Step 2: same negeri + DUN
    add_matches(require_negeri=True, match_field="dun")

    # Step 3: same negeri + PPD
    add_matches(require_negeri=True, match_field="ppd")

    # Step 4: same negeri (any)
    add_matches(require_negeri=True, match_field=None)

    senarai = [
        SekolahBerdekatanItem(
            namaSekolah=other.namaSekolah,
            kodSekolah=other.kodSekolah,
            bandarSurat=other.bandarSurat,
            negeri=other.negeri,
        )
        for other in selected[:5]
    ]

    return SekolahBerdekatan(senarai=senarai)


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

    raw_docs = list(collection.find({}))
    records: list[tuple[Dict[str, Any], Sekolah]] = []

    for raw in raw_docs:
        try:
            sekolah = Sekolah.model_validate(raw)
        except ValidationError as exc:  # pragma: no cover - defensive logging
            logger.warning("Skipping sekolah document due to validation error: %s", exc)
            continue
        records.append((raw, sekolah))

    all_schools = [sekolah for _, sekolah in records]

    documents: List[Dict[str, Any]] = []
    for raw, sekolah in records:
        nearby = _build_nearby_schools(sekolah, all_schools)
        document = _build_entiti_document(raw, sekolah, nearby)
        documents.append(document)
    return documents


__all__ = ["compute_entiti_sekolah", "ENTITI_SEKOLAH_COLLECTION"]
