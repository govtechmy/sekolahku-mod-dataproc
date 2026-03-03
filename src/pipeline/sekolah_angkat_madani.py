"""Ingest Sekolah Angkat Madani data from S3 into MongoDB.

Source: a single file named `sekolah_angkat_madani_filename` stored under `s3_prefix_sekolah_angkat_madani` in the dataproc bucket.
Columns: NEGERI, PPD, KOD SEKOLAH, NAMA SEKOLAH.
"""

from __future__ import annotations

import io
import logging
from typing import Any, Dict, Iterable

import pandas as pd
from botocore.exceptions import ClientError, ResponseStreamingError
from pydantic import ValidationError
from pymongo import MongoClient
from pymongo.collection import Collection
from pymongo.errors import OperationFailure

from src.config import Settings, get_settings
from src.core.s3 import _read_csv_from_s3
from src.models.sekolah_angkat_madani import SekolahAngkatMadani

logger = logging.getLogger(__name__)


def _read_excel_from_s3(bucket: str, key: str) -> pd.DataFrame:
	from src.core.s3 import s3  # imported lazily to match existing usage pattern

	try:
		response = s3.get_object(Bucket=bucket, Key=key)
	except ClientError as exc:
		raise RuntimeError(f"Error reading s3://{bucket}/{key}: {exc}") from exc

	try:
		return pd.read_excel(io.BytesIO(response["Body"].read()), dtype=str).fillna("")
	except ResponseStreamingError as exc:
		logger.error("Streaming error while reading s3://%s/%s: %s", bucket, key, exc)
		raise


def _load_rows(settings: Settings) -> Iterable[Dict[str, Any]]:
	bucket = settings.s3_bucket_dataproc
	if not bucket:
		raise RuntimeError("s3_bucket_dataproc must be set in settings")
	key = f"{settings.s3_prefix_sekolah_angkat_madani}/{settings.sekolah_angkat_madani_filename}"

	if key.lower().endswith(".csv"):
		df = _read_csv_from_s3(bucket, key)
	else:
		df = _read_excel_from_s3(bucket, key)

	# Normalise column headers to avoid trailing spaces/mismatches (e.g. "NEGERI ")
	df = df.rename(columns=lambda c: str(c).strip())

	total_rows = df.shape[0]
	logger.info(f"Sekolah Angkat Madani file loaded from s3://{bucket}/{key}: {total_rows} rows")
	return df.to_dict(orient="records")


def _collect_documents(settings: Settings) -> tuple[list[dict[str, Any]], list[dict[str, Any]], int]:
	documents: list[dict[str, Any]] = []
	errors: list[dict[str, Any]] = []
	total = 0

	for index, row in enumerate(_load_rows(settings), start=1):
		total += 1

		try:
			sekolah = SekolahAngkatMadani.model_validate(row)
		except ValidationError as exc:
			messages = [f"{'.'.join(map(str, err.get('loc', [])))}: {err.get('msg')}" for err in exc.errors()]
			errors.append({"row": index, "error": "; ".join(messages) or str(exc)})
			continue

		document = sekolah.to_document()
		documents.append(document)

	return documents, errors, total


def _get_collection(settings: Settings) -> Collection:
	client = MongoClient(settings.mongo_uri)
	db = client[settings.db_name]
	return db[SekolahAngkatMadani.collection_name]


def run_sekolah_angkat_madani(settings: Settings) -> dict[str, Any]:
	logger.info("Starting Sekolah Angkat Madani ingestion")
	collection = _get_collection(settings)
	database = collection.database

	documents, errors, total = _collect_documents(settings)

	inserted = 0
	deleted = 0
	staging_name = f"{SekolahAngkatMadani.collection_name}_staging"
	target_name = SekolahAngkatMadani.collection_name

	try:
		logger.info("Replacing collection '%s' via staging: total=%s, errors=%s", target_name, total, len(errors),)

		# Prepare staging collection
		if staging_name in database.list_collection_names():
			database.drop_collection(staging_name)

		staging = database[staging_name]
		previous_count = collection.count_documents({})

		if documents:
			insert_result = staging.insert_many(documents, ordered=False)
			inserted = len(getattr(insert_result, "inserted_ids", []) or [])

		# Atomically swap staging into place
		staging.rename(target_name, dropTarget=True)
		deleted = previous_count

	except OperationFailure as exc:
		if exc.code == 13:
			logger.error("MongoDB authentication failed. Ensure your MONGO_URI includes the correct credentials and authSource if required.")
			raise RuntimeError("MongoDB authentication failed; check credentials and auth source") from exc
		logger.error("MongoDB operation failed: %s", exc)
		raise
	finally:
		# Clean up any leftover staging collection on failure cases
		if staging_name in database.list_collection_names() and staging_name != target_name:
			database.drop_collection(staging_name)

	summary = {
		"collection": target_name,
		"total": total,
		"processed": total,
		"failed": len(errors),
		"errors": errors,
		"inserted": inserted,
		"deleted": deleted,
	}
	logger.info("Sekolah Angkat Madani ingestion completed: processed=%s, inserted=%s, deleted=%s, failed=%s", total, inserted, deleted, len(errors),)
	return summary


def run_with_overrides(**overrides: Any) -> dict[str, Any]:
	settings = get_settings().model_copy(update=overrides)
	return run_sekolah_angkat_madani(settings)


__all__ = ["run_sekolah_angkat_madani", "run_with_overrides"]
