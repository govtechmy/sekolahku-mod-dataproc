from __future__ import annotations
from typing import Any, Dict
from pymongo.collection import Collection


def _group_counts(collection: Collection, field: str) -> Dict[str, int]:
    pipeline = [
        {"$group": {"_id": {"$ifNull": [f"${field}", "UNKNOWN"]}, "count": {"$sum": 1}}},
        {"$sort": {"count": -1}},
    ]
    return {doc["_id"]: doc["count"] for doc in collection.aggregate(pipeline)}


def compute_StatistikSekolah(collection: Collection) -> Dict[str, Any]:
    """Aggregate sekolah statistics for StatistikSekolah collection."""
    total = collection.count_documents({})

    bantuan_counts = _group_counts(collection, "bantuan")
    bilSesi_counts = _group_counts(collection, "bilSesi")
    lokasi_counts = _group_counts(collection, "lokasi")

    bantuan_unknown = sum(v for k, v in bantuan_counts.items() if k not in {"SK", "SBK"})
    bilSesi_unknown = sum(v for k, v in bilSesi_counts.items() if k not in {"1 Sesi", "2 Sesi"})
    lokasi_unknown = sum(v for k, v in lokasi_counts.items() if k not in {"Bandar", "Luar Bandar"})

    data = {
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

    return {"data": data}


__all__ = ["compute_StatistikSekolah"]
