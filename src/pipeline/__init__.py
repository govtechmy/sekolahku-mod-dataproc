"""Pipeline package entrypoints."""
from .ingestion import run
from .statistics import run_statistics, run_statistics_dict
from .entitiSekolah import run_entiti_sekolah, run_entiti_sekolah_dict

__all__ = [
	"run",
	"run_statistics",
	"run_statistics_dict",
	"run_entiti_sekolah",
	"run_entiti_sekolah_dict",
]
