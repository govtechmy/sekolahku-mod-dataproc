from __future__ import annotations

import logging
from collections import defaultdict
from typing import Any, Dict, List

from pydantic import ValidationError
from pymongo.collection import Collection

from src.models.analitik_sekolah import AnalitikSekolah, AnalitikSekolahData

logger = logging.getLogger(__name__)

ANALITIK_SEKOLAH_COLLECTION = AnalitikSekolah.collection_name


def _normalize_field_expression(field: str) -> Dict[str, Any]:
    """Return Mongo aggregation expression that normalizes a string field."""

    return {
        "$let": {
            "vars": {"raw": {"$ifNull": [f"${field}", ""]}},
            "in": {
                "$let": {
                    "vars": {
                        "text": {
                            "$trim": {
                                "input": {
                                    "$toString": "$$raw"
                                }
                            }
                        }
                    },
                    "in": {
                        "$cond": [
                            {"$eq": ["$$text", ""]},
                            "TIADA MAKLUMAT",
                            {"$toUpper": "$$text"},
                        ]
                    },
                }
            },
        }
    }


def _dimension_facet(field: str) -> List[Dict[str, Any]]:
    """Build facet stages that aggregate counts for a specific field."""

    normalized_field = _normalize_field_expression(field)
    return [
        {
            "$group": {
                "_id": normalized_field,
                "total": {"$sum": 1},
            }
        },
        {
            "$project": {
                "_id": 0,
                "jenis": "$_id",
                "total": 1,
            }
        },
        {"$sort": {"total": -1, "jenis": 1}},
    ]


def _build_aggregation_pipeline() -> List[Dict[str, Any]]:
    """Construct Mongo aggregation pipeline for analytics computation."""

    student_sum = {
        "$add": [
            {"$ifNull": ["$enrolmen", 0]},
            {"$ifNull": ["$enrolmenPrasekolah", 0]},
            {"$ifNull": ["$enrolmenKhas", 0]},
        ]
    }

    return [
        {
            "$match": {
                "status": "ACTIVE"
            }
        },
        {
            "$facet": {
                "metadata": [
                    {
                        "$group": {
                            "_id": None,
                            "jumlahSekolah": {"$sum": 1},
                            "jumlahGuru": {"$sum": {"$ifNull": ["$guru", 0]}},
                            "jumlahPelajar": {"$sum": student_sum},
                        }
                    }
                ],
                "jenisLabel": _dimension_facet("jenisLabel"),
                "bantuan": _dimension_facet("bantuan"),
            }
        },
        {
            "$project": {
                "metadata": {"$ifNull": [{"$arrayElemAt": ["$metadata", 0]}, {}]},
                "jenisLabel": 1,
                "bantuan": 1,
            }
        },
    ]


def _convert_buckets_to_items(buckets: List[Dict[str, Any]], total: int) -> List[Any]:
    """Convert aggregation buckets into AnalitikItem entries."""

    counter: defaultdict[str, int] = defaultdict(int)
    for bucket in buckets:
        jenis = bucket.get("jenis")
        if not jenis:
            continue

        try:
            counter[str(jenis)] = int(bucket.get("total", 0) or 0)
        except (TypeError, ValueError):
            logger.debug("Skipping bucket %s due to invalid total", bucket)
            continue

    return AnalitikSekolah._convert_to_analitik_items(counter, total)


def _compute_institusi_totals(institusi_collection: Collection | None) -> dict[str, int]:
    """
    Compute total guru and enrolmenPraSekolah counts from ACTIVE institusi.
    """
    if institusi_collection is None:
        return {"guru": 0, "enrolmenPraSekolah": 0}

    pipeline = [
        {"$match": {"status": "ACTIVE"}},
        {
            "$group": {
                "_id": None,
                "guru": {"$sum": {"$ifNull": ["$guru", 0]}},
                "enrolmenPraSekolah": {"$sum": {"$ifNull": ["$enrolmenPraSekolah", 0]}},
            }
        },
    ]

    result = next(institusi_collection.aggregate(pipeline, allowDiskUse=False), None)
    if not result:
        return {"guru": 0, "enrolmenPraSekolah": 0}

    return {
        "guru": int(result.get("guru") or 0),
        "enrolmenPraSekolah": int(result.get("enrolmenPraSekolah") or 0),
    }


def compute_analitik_sekolah(
    sekolah_collection: Collection,
    institusi_collection: Collection | None = None,
) -> List[Dict[str, Any]]:
    """Project sekolah collection into the AnalitikSekolah aggregation view.

    When ``institusi_collection`` is provided, guru and enrolmenPraSekolah totals include ACTIVE
    institusi documents as well as sekolah.
    """

    pipeline = _build_aggregation_pipeline()
    result = next(sekolah_collection.aggregate(pipeline, allowDiskUse=True), None)

    if not result:
        logger.warning("No sekolah documents found in collection")
        return []

    metadata = result.get("metadata") or {}
    jumlah_sekolah = int(metadata.get("jumlahSekolah") or 0)

    if jumlah_sekolah == 0:
        logger.warning("No sekolah documents found for analytics processing")
        return []
    
    jumlah_guru = int(metadata.get("jumlahGuru") or 0)
    jumlah_pelajar = int(metadata.get("jumlahPelajar") or 0)

    institusi_totals = _compute_institusi_totals(institusi_collection)
    jumlah_guru += institusi_totals.get("guru", 0)
    # enrolmenPraSekolah contributes to overall pelajar total for institusi
    jumlah_pelajar += institusi_totals.get("enrolmenPraSekolah", 0)

    data = AnalitikSekolahData(
        jenisLabel=_convert_buckets_to_items(result.get("jenisLabel", []), jumlah_sekolah),
        bantuan=_convert_buckets_to_items(result.get("bantuan", []), jumlah_sekolah),
    )

    try:
        analitik_sekolah = AnalitikSekolah(
            id="1",
            jumlahSekolah=jumlah_sekolah,
            jumlahGuru=jumlah_guru,
            jumlahPelajar=jumlah_pelajar,
            data=data,
        )
    except ValidationError as exc:
        logger.error("Failed to validate analytics document: %s", exc)
        return []

    document = analitik_sekolah.to_document()
    document["_id"] = "1"
    return [document]


__all__ = ["compute_analitik_sekolah", "ANALITIK_SEKOLAH_COLLECTION"]