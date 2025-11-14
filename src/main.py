"""CLI entrypoint for data ingestion."""
from __future__ import annotations

import argparse
import logging
from typing import Any, Dict

from src.config.settings import Settings, get_settings
from src.pipelines.ingest_pipeline import run as run_pipeline


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
    return parser.parse_args()


def _collect_overrides(args: argparse.Namespace) -> Dict[str, Any]:
    overrides: Dict[str, Any] = {}
    if args.source:
        overrides["source"] = args.source
    if args.csv_path:
        overrides["csv_path"] = args.csv_path
    if args.gsheet_id:
        overrides["gsheet_id"] = args.gsheet_id
    if args.gsheet_worksheet:
        overrides["gsheet_worksheet_name"] = args.gsheet_worksheet
    if args.google_credentials:
        overrides["google_credentials_path"] = args.google_credentials
    if args.mongo_uri:
        overrides["mongo_uri"] = args.mongo_uri
    if args.db_name:
        overrides["db_name"] = args.db_name
    if args.batch_size is not None:
        overrides["batch_size"] = args.batch_size
    if args.dry_run:
        overrides["dry_run_env"] = 1
    return overrides


def configure_settings(args: argparse.Namespace) -> Settings:
    base = get_settings()
    overrides = _collect_overrides(args)
    if overrides:
        base = base.model_copy(update=overrides)
    return base


def main() -> None:
    args = parse_args()
    logging.basicConfig(level=getattr(logging, args.log_level.upper(), logging.INFO))
    settings = configure_settings(args)
    result = run_pipeline(settings)
    print("Ingestion summary:", result)


if __name__ == "__main__":
    main()
