"""Statistics aggregation public API."""
from .aggEntitiSekolah import compute_entiti_sekolah, ENTITI_SEKOLAH_COLLECTION
from .aggAnalitikSekolah import compute_analitik_sekolah, ANALITIK_SEKOLAH_COLLECTION

__all__ = [
    "compute_entiti_sekolah",
    "ENTITI_SEKOLAH_COLLECTION",
    "compute_analitik_sekolah",
    "ANALITIK_SEKOLAH_COLLECTION",
]