# sekolahku-mod-dataproc

Data processing & ingestion module for the Sekolahku project. This service ingests school metadata (CSV / Google Sheets) into MongoDB using a validated Pydantic schema.

## Folder Structure

```
src/
	adapters/
		csv.py                   # CSV loader (streaming)
		sheets.py                # Google Sheets adapter
	config/
		settings.py             # Environment-driven configuration
	db/
		mongo.py                # Mongo client factory
		operations.py           # Bulk write helpers (chunk & replace)
	models/
		schema.py               # Pydantic schema for school documents
	pipelines/
		ingest_pipeline.py      # High-level ingestion orchestration
	services/
		ingestion.py            # Extract/validate/load implementation
main.py                      # CLI entrypoint delegating to pipeline
docs/                        # Source CSV + project docs
```

Flow overview:
1. Adapters stream raw rows from CSV or Google Sheets.
2. `School` Pydantic model validates and normalises the dataset.
3. Mongo helpers batch-replace the `schools` collection (supports dry-run).
4. Pipeline logs a summary that the CLI prints to stdout.

## Requirements

Python 3.11+

Install dependencies:
```bash
pip install -r requirements.txt
```

## Environment Variables

Create a `.env` (or use shell exports):

```
MONGO_URI=mongodb://localhost:27017
DB_NAME=sekolahku
GSHEET_ID=<optional-google-sheet-id>
GSHEET_WORKSHEET_NAME=<optional-worksheet-name>
SOURCE=csv   # or gsheet
CSV_PATH=data/sekolah.csv
```

If your Mongo instance requires authentication, embed the `username:password@` portion and any `authSource` query params directly in `MONGO_URI`.

If using Google Sheets, place your `service_account.json` (not committed) in project root or set `GOOGLE_APPLICATION_CREDENTIALS`.

## Running Ingestion

```bash
python main.py --source csv --dry-run
python main.py --source gsheet --gsheet-id <ID> --google-credentials service_account.json
```

CLI Arguments:
* `--source` csv|gsheet (override configured source)
* `--csv-path` override CSV location when `--source csv`
* `--gsheet-id` / `--gsheet-worksheet` select worksheet when `--source gsheet`
* `--mongo-uri`, `--db-name`, `--batch-size` tune database writes (URI can include credentials)
* `--dry-run` validate only (no DB writes)
* `--log-level` adjust logging verbosity (default `INFO`)

## Data Model (TBD)

| Field | Source Column | Type | Notes |
|-------|---------------|------|-------|
| `kodsekolah` | KODSEKOLAH | str | Primary key / unique index |
| `namasekolah` | NAMASEKOLAH | str | |
| `negeri` | NEGERI | str | |
| `ppd` | PPD | str | |
| `parlimen` | PARLIMEN | str | |
| `dun` | DUN | str | |
| `peringkat` | PERINGKAT | str | |
| `jenis_label` | JENIS/LABEL | str | slash normalized |
| `alamatsurat` | ALAMATSURAT | str | |
| `poskodsurat` | POSKODSURAT | str | kept as string (leading zeros safe) |
| `bandarsurat` | BANDARSURAT | str | |
| `notelefon` | NOTELEFON | Optional[str] | "TIADA" → None |
| `nofax` | NOFAX | Optional[str] | "TIADA" → None |
| `email` | EMAIL | Optional[EmailStr] | |
| `lokasi` | LOKASI | Optional[str] | Bandar/Luar Bandar |
| `gred` | GRED | Optional[str] | |
| `bantuan` | BANTUAN | Optional[str] | SK / etc |
| `bilsesi` | BILSESI | Optional[str] | |
| `sesi` | SESI | Optional[str] | |
| `enrolmen_prasekolah` | ENROLMEN PRASEKOLAH | Optional[int] | blanks → None |
| `enrolmen` | ENROLMEN | Optional[int] | |
| `enrolmen_khas` | ENROLMEN KHAS | Optional[int] | |
| `guru` | GURU | Optional[int] | |
| `prasekolah` | PRASEKOLAH | Optional[bool] | ADA/TIADA |
| `integrasi` | INTEGRASI | Optional[bool] | ADA/TIADA |
| `koordinat_x` | KOORDINATXX | Optional[float] | |
| `koordinat_y` | KOORDINATYY | Optional[float] | |
| `skm_leq_150` | SKM<=150 | Optional[bool] | YA → True |


## License
TBD

