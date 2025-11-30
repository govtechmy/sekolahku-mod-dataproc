from __future__ import annotations

import logging
from collections import defaultdict
from typing import Any, Dict, List

from pydantic import ValidationError
from pymongo.collection import Collection

from src.models.analitikSekolah import AnalitikSekolah, AnalitikSekolahData

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


def compute_analitik_sekolah(collection: Collection) -> List[Dict[str, Any]]:
    """Project sekolah collection into the AnalitikSekolah aggregation view."""

    pipeline = _build_aggregation_pipeline()
    result = next(collection.aggregate(pipeline, allowDiskUse=True), None)

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

    data = AnalitikSekolahData(
        jenisLabel=_convert_buckets_to_items(result.get("jenisLabel", []), jumlah_sekolah),
        bantuan=_convert_buckets_to_items(result.get("bantuan", []), jumlah_sekolah),
    )

    try:
        analitik = AnalitikSekolah(
            id="ALL",
            jumlahSekolah=jumlah_sekolah,
            jumlahGuru=jumlah_guru,
            jumlahPelajar=jumlah_pelajar,
            data=data,
        )
    except ValidationError as exc:
        logger.error("Failed to validate analytics document: %s", exc)
        return []

    document = analitik.to_document()
    return [document]


__all__ = ["compute_analitik_sekolah", "ANALITIK_SEKOLAH_COLLECTION"]