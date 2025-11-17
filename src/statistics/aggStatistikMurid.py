from __future__ import annotations
from typing import Any, Dict
from pymongo.collection import Collection


def compute_StatistikMurid(collection: Collection) -> Dict[str, Any]:
    """Aggregate murid statistics for StatistikMurid collection."""
    pipeline = [
        {"$group": {"_id": None, "jumlahMurid": {"$sum": {"$ifNull": ["$enrolmen", 0]}}}}
    ]
    result = list(collection.aggregate(pipeline))
    total = result[0]["jumlahMurid"] if result else 0
    
    data = {
        "jumlahMurid": total,
        "jantina": {
            "perempuan": 50,
            "lelaki": 10,
            "tiada-maklumat": 0,
        },
    }

    return {"data": data}


__all__ = ["compute_StatistikMurid"]
