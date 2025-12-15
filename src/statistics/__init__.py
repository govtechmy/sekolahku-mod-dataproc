"""Statistics aggregation public API."""
from .agg_entiti_sekolah import compute_entiti_sekolah, ENTITI_SEKOLAH_COLLECTION
from .agg_analitik_sekolah import compute_analitik_sekolah, ANALITIK_SEKOLAH_COLLECTION

__all__ = [
    "compute_entiti_sekolah",
    "ENTITI_SEKOLAH_COLLECTION",
    "compute_analitik_sekolah",
    "ANALITIK_SEKOLAH_COLLECTION",
]