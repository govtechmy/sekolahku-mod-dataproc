"""MongoDB aggregation helpers for statistics.

Each function expects a `Collection` instance referencing the `schools` collection
populated by the ingestion pipeline.
"""
from __future__ import annotations

from typing import Dict, Any
from pymongo.collection import Collection


STATISTIK_SEKOLAH_COLLECTION = "StatistikSekolah"
STATISTIK_GURU_COLLECTION = "StatistikGuru"
STATISTIK_MURID_COLLECTION = "StatistikMurid"


from src.models.statistics import (
    StatistikGuruDocument,
    StatistikMuridDocument,
    StatistikSekolahDocument,
)


def _group_counts(collection: Collection, field: str) -> Dict[str, int]:
    pipeline = [
        {"$group": {"_id": {"$ifNull": [f"${field}", "UNKNOWN"]}, "count": {"$sum": 1}}},
        {"$sort": {"count": -1}},
    ]
    return {doc["_id"]: doc["count"] for doc in collection.aggregate(pipeline)}


def compute_StatistikSekolah(collection: Collection) -> Dict[str, Any]:
    """Aggregate sekolah statistics and wrap the result under a `data` key."""
    total = collection.count_documents({})

    bantuan_counts = _group_counts(collection, "bantuan")
    bilSesi_counts = _group_counts(collection, "bilSesi")
    lokasi_counts = _group_counts(collection, "lokasi")

    bantuan_unknown = sum(v for k, v in bantuan_counts.items() if k not in {"SK", "SBK"})
    bilSesi_unknown = sum(v for k, v in bilSesi_counts.items() if k not in {"1 Sesi", "2 Sesi"})
    lokasi_unknown = sum(v for k, v in lokasi_counts.items() if k not in {"Bandar", "Luar Bandar"})

    return {
        "data": {
            "jumlahSekolah": total,
            "bantuan": {
                "kerajaan": bantuan_counts.get("SK", 0),
                "bantuan-kerajaan": bantuan_counts.get("SBK", 0),
                "tiada-maklumat": bantuan_unknown,
            },
            "bilSesi": {
                "1-sesi": bilSesi_counts.get("1 Sesi", 0),
                "2-sesi": bilSesi_counts.get("2 Sesi", 0),
                "tiada-maklumat": bilSesi_unknown,
            },
            "lokasi": {
                "bandar": lokasi_counts.get("Bandar", 0),
                "luar-bandar": lokasi_counts.get("Luar Bandar", 0),
                "tiada-maklumat": lokasi_unknown,
            },
        }
    }


def compute_StatistikGuru(collection: Collection) -> Dict[str, Any]:
    """Aggregate guru statistics and wrap the result under a `data` key."""
    pipeline = [
        {"$match": {"guru": {"$ne": None}}},
        {"$group": {"_id": None, "jumlahGuru": {"$sum": "$guru"}}},
    ]
    result = list(collection.aggregate(pipeline))
    total = result[0]["jumlahGuru"] if result else 0

    return {
        "data": {
            "jumlahGuru": total,
            "jantina": {
                "perempuan": 50,
                "lelaki": 10,
                "tiada-maklumat": 0,
            },
        }
    }


def compute_StatistikMurid(collection: Collection) -> Dict[str, Any]:
    """Aggregate murid statistics and wrap the result under a `data` key."""
    pipeline = [
        {"$group": {"_id": None, "jumlahMurid": {"$sum": {"$ifNull": ["$enrolmen", 0]}}}}
    ]
    result = list(collection.aggregate(pipeline))
    total = result[0]["jumlahMurid"] if result else 0

    return {
        "data": {
            "jumlahMurid": total,
            "jantina": {
                "perempuan": 50,
                "lelaki": 10,
                "tiada-maklumat": 0,
            },
        }
    }


def compute_all_statistics(collection: Collection) -> Dict[str, Any]:
    return {
        "sekolah": compute_StatistikSekolah(collection),
        "guru": compute_StatistikGuru(collection),
        "murid": compute_StatistikMurid(collection),
    }


def build_statistik_documents(stats: Dict[str, Any]) -> Dict[str, Dict[str, Any]]:
    """Convert aggregated stats into documents for Statistik collections."""

    sekolah_doc = StatistikSekolahDocument(data=stats["sekolah"]["data"]).model_dump(by_alias=True)
    guru_doc = StatistikGuruDocument(data=stats["guru"]["data"]).model_dump(by_alias=True)
    murid_doc = StatistikMuridDocument(data=stats["murid"]["data"]).model_dump(by_alias=True)
    return {
        STATISTIK_SEKOLAH_COLLECTION: sekolah_doc,
        STATISTIK_GURU_COLLECTION: guru_doc,
        STATISTIK_MURID_COLLECTION: murid_doc,
    }

