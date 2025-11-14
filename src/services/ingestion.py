"""Data ingestion service."""
from __future__ import annotations

import logging
from typing import Iterable, Dict, Any

from pydantic import ValidationError
from pymongo.errors import OperationFailure

from src.adapters.csv import load as load_csv  # type: ignore[import-not-found]
from src.adapters.sheets import load as load_sheet  # type: ignore[import-not-found]
from src.config.settings import Settings  # type: ignore[import-not-found]
from src.db.mongo import get_collection  # type: ignore[import-not-found]
from src.db.operations import replace_documents  # type: ignore[import-not-found]
from src.models import School  # type: ignore[import-not-found]

logger = logging.getLogger(__name__)


def _iter_raw_rows(settings: Settings) -> Iterable[Dict[str, Any]]:
    source = settings.source.lower()
    if source == "csv":
        logger.info("Loading data from CSV: %s", settings.csv_path)
        return load_csv(settings.csv_path)
    if source in {"gsheet", "sheets", "google"}:
        if settings.gsheet_id is None:
            raise ValueError("GSHEET_ID must be set when SOURCE is gsheet")
        logger.info(
            "Loading data from Google Sheet %s (%s)",
            settings.gsheet_id,
            settings.gsheet_worksheet_name,
        )
        return load_sheet(settings.gsheet_id, settings.gsheet_worksheet_name, settings.google_credentials_path)
    raise ValueError(f"Unsupported source '{settings.source}'")


def _iter_schools(settings: Settings) -> Iterable[School]:
    for idx, row in enumerate(_iter_raw_rows(settings), start=1):
        try:
            school = School.model_validate(row)
        except ValidationError as exc:  # pragma: no cover - logging only
            logger.warning("Row %s failed validation: %s", idx, exc)
            continue
        yield school


def ingest(settings: Settings) -> dict[str, int]:
    logger.info("Starting ingestion (dry_run=%s)", settings.dry_run)
    collection = get_collection(settings, School.collection_name)
    documents = (school.to_document() for school in _iter_schools(settings))
    try:
        result = replace_documents(
            collection,
            documents,
            batch_size=settings.batch_size,
            dry_run=settings.dry_run,
        )
    except OperationFailure as exc:
        if exc.code == 13:
            logger.error(
                "MongoDB authentication failed. Ensure your MONGO_URI includes the"
                " correct username, password, and authSource (if required)."
            )
            raise RuntimeError("MongoDB authentication failed; check credentials and auth source") from exc
        logger.error("MongoDB operation failed: %s", exc)
        raise
    logger.info("Ingestion finished: %s", result)
    return result
