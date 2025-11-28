from __future__ import annotations

import argparse
import logging

from src.config.settings import Settings, get_settings
from src.pipeline import (
    run as run_pipeline,
    run_entiti_sekolah_dict,
    run_analitik_dict,
)

logger = logging.getLogger(__name__)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Ingest school data into MongoDB")
    parser.add_argument("--source", choices=["csv", "gsheet"], help="Choose data source (default from env)")
    parser.add_argument("--csv-path", help="Path to CSV file when source=csv")
    parser.add_argument("--gsheet-id", help="Google Sheet ID when source=gsheet")
    parser.add_argument("--gsheet-worksheet", help="Google Sheet worksheet name")
    parser.add_argument("--google-credentials", help="Path to Google service account json")
    parser.add_argument("--mongo-uri", help="Mongo connection string")
    parser.add_argument("--db-name", help="Mongo database name")
    parser.add_argument("--batch-size", type=int, help="Insert batch size")
    parser.add_argument("--log-level", default="INFO", help="Logging level (e.g., INFO, DEBUG)")
    parser.add_argument("--entiti", action="store_true", help="Compute EntitiSekolah aggregation into separate collection")
    parser.add_argument("--analitik", action="store_true", help="Compute Analitik aggregation after ingestion")
    return parser.parse_args()


def configure_settings(_: argparse.Namespace) -> Settings:
    """Return settings from environment."""
    return get_settings()


def main() -> None:
    args = parse_args()
    logging.basicConfig(level=getattr(logging, args.log_level.upper(), logging.INFO))
    settings = configure_settings(args)
    
    if args.entiti:
        entiti = run_entiti_sekolah_dict(settings)
        logger.info("Entiti summary: %s", entiti)
        return

    if args.analitik:
        analitik = run_analitik_dict(settings)
        logger.info("Analitik summary: %s", analitik)
        return

    # -------------------------
    # Run ingestion pipeline if no specific aggregation flags are provided
    # -------------------------
    result = run_pipeline(settings)
    logger.info("Ingestion summary: %s", result)

    entiti = run_entiti_sekolah_dict(settings)
    logger.info("Entiti summary: %s", entiti)
    analitik = run_analitik_dict(settings)
    logger.info("Analitik summary: %s", analitik)

if __name__ == "__main__":
    main()
