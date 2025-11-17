from __future__ import annotations

import argparse
import logging

from src.config.settings import Settings, get_settings
from src.pipeline import run as run_pipeline, run_statistics_dict


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
    parser.add_argument("--dry-run", action="store_true", help="Process without writing to database")
    parser.add_argument("--log-level", default="INFO", help="Logging level (e.g., INFO, DEBUG)")
    parser.add_argument("--statistics", action="store_true", help="Compute statistics collections after ingestion",)
    return parser.parse_args()


def configure_settings(_: argparse.Namespace) -> Settings:
    """Return settings from environment."""
    return get_settings()


def main() -> None:
    args = parse_args()
    logging.basicConfig(level=getattr(logging, args.log_level.upper(), logging.INFO))
    settings = configure_settings(args)
    result = run_pipeline(settings)
    print("Ingestion summary:", result)

    if args.statistics:
        stats = run_statistics_dict(settings)
        print("Statistics summary:", stats)


if __name__ == "__main__":
    main()
