"""Pipeline package entrypoints."""
from .ingestion import run
from .statistics import run_statistics, run_statistics_dict

__all__ = ["run", "run_statistics", "run_statistics_dict"]
