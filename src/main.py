from __future__ import annotations

import argparse
import logging

from src.config.settings import Settings, get_settings
from src.pipeline import (
    run as run_pipeline,
    run_entiti_sekolah_dict,
    run_analitik_dict,
    run_negeri_parlimen_kod_sekolah,
)
from src.service.polygons.load_opendosm_negeri import main as load_negeri_polygons
from src.service.polygons.load_opendosm_parlimen import main as load_parlimen_polygons

logger = logging.getLogger(__name__)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Ingest school data into MongoDB")
    parser.add_argument("--csv-path", help="Path to the CSV file")
    parser.add_argument("--mongo-uri", help="Mongo connection string")
    parser.add_argument("--db-name", help="Mongo database name")
    parser.add_argument("--batch-size", type=int, help="Insert batch size")
    parser.add_argument("--log-level", default="INFO", help="Logging level (e.g., INFO, DEBUG)")
    parser.add_argument("--entiti", action="store_true", help="Compute EntitiSekolah aggregation into separate collection")
    parser.add_argument("--analitik", action="store_true", help="Compute Analitik aggregation after ingestion")
    parser.add_argument("--negeri-parlimen-kod-sekolah", action="store_true", help="Populate NegeriParlimenKodSekolah collection from Sekolah and exit")
    parser.add_argument("--load-polygons", action="store_true", help="Load OpenDOSM polygon data from S3 into MongoDB and exit")
    return parser.parse_args()


def configure_settings(_: argparse.Namespace) -> Settings:
    """Return settings from environment."""
    return get_settings()

def run_ingest(settings: Settings | None = None) -> dict:
    """
    Run the full ingestion pipeline including all aggregations.
    
    This function can be called programmatically from other modules (e.g., api.py)
    without needing command-line arguments.
    
    Args:
        settings: Optional Settings object. If None, loads from environment.
    
    Returns:
        Dictionary containing summary of all pipeline operations:
        - ingestion: Main ingestion results
        - entiti: EntitiSekolah aggregation results
        - negeri_parlimen_kod_sekolah: NegeriParlimenKodSekolah results
        - analitik: Analitik aggregation results (if data changed)
    """
    if settings is None:
        settings = get_settings()
    
    # -------------------------
    # Run ingestion pipeline
    # -------------------------
    result = run_pipeline(settings)
    logger.info("Ingestion summary: %s", result)

    entiti = run_entiti_sekolah_dict(settings)
    logger.info("Entiti summary: %s", entiti)

    negeri_parlimen_kod_sekolah_summary = run_negeri_parlimen_kod_sekolah(settings)
    logger.info("NegeriParlimenKodSekolah summary: %s", negeri_parlimen_kod_sekolah_summary)

    if result["inserted"] == 0 and result["updated"] == 0 and result["inactivated"] == 0:
        logger.info("No data changes detected; skipping Analitik run")
        analitik = None
    else:
        analitik = run_analitik_dict(settings)
        logger.info("Analitik summary: %s", analitik)
    
    return {
        "ingestion": result,
        "entiti": entiti,
        "negeri_parlimen_kod_sekolah": negeri_parlimen_kod_sekolah_summary,
        "analitik": analitik,
    }

def main() -> None:
    args = parse_args()
    logging.basicConfig(level=getattr(logging, args.log_level.upper(), logging.INFO))
    settings = configure_settings(args)

    if getattr(args, "load_polygons", False):
        logger.info("=" * 60)
        logger.info("LOADING OPENDOSM POLYGONS FROM S3 TO MONGODB")
        logger.info("=" * 60)
        
        negeri_summary = load_negeri_polygons()
        logger.info("Negeri summary: %s", negeri_summary)
        
        parlimen_summary = load_parlimen_polygons()
        logger.info("Parlimen summary: %s", parlimen_summary)
        return

    if getattr(args, "negeri_parlimen_kod_sekolah", False):
        summary = run_negeri_parlimen_kod_sekolah(settings)
        logger.info("NegeriParlimenKodSekolah summary: %s", summary)
        return

    if args.entiti:
        entiti = run_entiti_sekolah_dict(settings)
        logger.info("Entiti summary: %s", entiti)
        return

    if args.analitik:
        analitik = run_analitik_dict(settings)
        logger.info("Analitik summary: %s", analitik)
        return

    run_ingest(settings)

if __name__ == "__main__":
    main()
