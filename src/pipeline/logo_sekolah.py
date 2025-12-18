from __future__ import annotations

import csv
import logging
import sys
from pathlib import Path
from typing import Iterable, List

from pymongo import MongoClient, UpdateOne

from src.config.settings import get_settings
from src.models.logo_sekolah import LogoSekolah


logger = logging.getLogger(__name__)

CSV_PATH = Path("/Users/asyiq/Documents/github/sekolahku-mod-dataproc/data/tbi_institusi_induk.csv")

csv.field_size_limit(sys.maxsize)


def _get_mongo_collection():
	"""Return the MongoDB collection for logo sekolah using global Settings.

	Uses:
	- settings.mongo_uri
	- settings.db_name
	- environment variable LOGO_SEKOLAH_COLLECTION (optional, default: LogoSekolah)
	"""

	settings = get_settings()

	mongo_uri = settings.mongo_uri
	db_name = settings.db_name

	if not mongo_uri or not db_name:
		raise RuntimeError(
			"Missing Mongo configuration: ensure MONGO_URI and DB_NAME are set in environment or secrets."
		)

	client = MongoClient(mongo_uri)
	db = client[db_name]
	collection_name = settings.logo_sekolah_collection
	return db[collection_name]


def _csv_rows_to_models(csv_path: Path) -> Iterable[LogoSekolah]:
	"""Read the CSV and yield LogoSekolah models.

	Expected headers:
	- KOD_INSTITUSI
	- NAMA_PENUH_INSTITUSI
	- LOGO
	"""

	with csv_path.open("r", newline="", encoding="utf-8-sig") as f:
		reader = csv.DictReader(f)
		for idx, row in enumerate(reader, start=1):
			kod = (
				row.get("KOD_INSTITUSI")
				or row.get("kod_institusi")
				or row.get("Kod_Institusi")
			)

			if not kod:
				# Skip malformed rows
				print("Row %d skipped: missing KOD_INSTITUSI", idx)
				continue

			model = LogoSekolah(
				KOD_INSTITUSI=(kod or "").strip(),
				NAMA_PENUH_INSTITUSI=(row.get("NAMA_PENUH_INSTITUSI") or "").strip(),
				LOGO=(row.get("LOGO") or "").strip(),
			)
			yield model


def upsert_logo_sekolah_from_csv(
	csv_path: Path | None = None,
	batch_size: int | None = None,
) -> int:
	"""Upsert logo sekolah data from CSV into MongoDB.

	- Uses KOD_INSTITUSI as _id and unique key.
	"""

	settings = get_settings()
	if batch_size is None:
		batch_size = settings.batch_size

	if csv_path is None:
		csv_path = CSV_PATH

	if not csv_path.exists():
		raise FileNotFoundError(f"CSV not found: {csv_path}")

	print(f"Starting upsert from data: {csv_path}")

	coll = _get_mongo_collection()
	ops: List[UpdateOne] = []
	total = 0
	processed = 0

	for model in _csv_rows_to_models(csv_path):
		doc = model.mongo_document()
		ops.append(
			UpdateOne(
				{"_id": doc["_id"]},
				{"$set": doc},
				upsert=True,
			)
		)
		total += 1
		processed += 1

		if processed % 1000 == 0:
			print(f"Total updated: ", processed)

		if len(ops) >= batch_size:
			coll.bulk_write(ops, ordered=False)
			ops.clear()

	if ops:
		coll.bulk_write(ops, ordered=False)

	return total


def run() -> None:
	"""Entry point to run the logo sekolah pipeline from CLI or orchestrator."""

	total = upsert_logo_sekolah_from_csv()
	print(f"Upserted {total} records into 'LogoSekolah' collection from tbi_institusi_induk.csv")


if __name__ == "__main__":
	run()

