# sekolahku-mod-dataproc

Data processing & ingestion module for the Sekolahku project. This service ingests school metadata from both CSV files and Google Sheets into MongoDB using a validated Pydantic schema.

## Folder Structure

```
src/
	config/
		settings.py             # Environment-driven configuration
	models/
		school.py               # Pydantic schema for school documents
	pipeline/
		ingestion.py            # Extract/validate/load implementation
main.py                      # CLI entrypoint delegating to pipeline
docs/                        # Project documentation
```

Flow overview:
1. Data is streamed from either CSV files or Google Sheets.
2. `School` Pydantic model validates and normalizes the dataset.
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
SOURCE=csv   # or gsheet
CSV_PATH=data/sekolah.csv
GSHEET_ID=<google-sheet-id>
GSHEET_WORKSHEET_NAME=<worksheet-name>
GOOGLE_APPLICATION_CREDENTIALS=service_account.json
```

If your Mongo instance requires authentication, embed the `username:password@` portion and any `authSource` query params directly in `MONGO_URI`.

## Running Ingestion

```bash
# For CSV source
python main.py --source csv --csv-path data/sekolah.csv --dry-run

# For Google Sheets source
python main.py --source gsheet --gsheet-id <ID> --google-credentials service_account.json
```

CLI Arguments:
* `--source` csv|gsheet (override configured source)
* `--csv-path` override CSV location when `--source csv`
* `--gsheet-id` / `--gsheet-worksheet` select worksheet when `--source gsheet`
* `--mongo-uri`, `--db-name`, `--batch-size` tune database writes (URI can include credentials)
* `--dry-run` validate only (no DB writes)
* `--log-level` adjust logging verbosity (default `INFO`)

## Data Model

| Field | Source Column | Type | Notes |
|-------|---------------|------|-------|
| `negeri` | NEGERI | Optional[str] | |
| `ppd` | PPD | Optional[str] | |
| `parlimen` | PARLIMEN | Optional[str] | |
| `dun` | DUN | Optional[str] | |
| `peringkat` | PERINGKAT | Optional[str] | |
| `jenisLabel` | JENIS/LABEL | Optional[str] | |
| `kodSekolah` | KODSEKOLAH | Optional[str] | School code |
| `namaSekolah` | NAMASEKOLAH | Optional[str] | |
| `alamatSurat` | ALAMATSURAT | Optional[str] | |
| `poskodSurat` | POSKODSURAT | Optional[int] | |
| `bandarSurat` | BANDARSURAT | Optional[str] | |
| `noTelefon` | NOTELEFON | Optional[str] | "TIADA" → None |
| `noFax` | NOFAX | Optional[str] | "TIADA" → None |
| `email` | EMAIL | Optional[EmailStr] | Normalized |
| `lokasi` | LOKASI | Optional[str] | |
| `gred` | GRED | Optional[str] | |
| `bantuan` | BANTUAN | Optional[str] | |
| `bilSesi` | BILSESI | Optional[str] | |
| `sesi` | SESI | Optional[str] | |
| `enrolmenPrasekolah` | ENROLMEN PRASEKOLAH | Optional[int] | |
| `enrolmen` | ENROLMEN | Optional[int] | |
| `enrolmenKhas` | ENROLMEN KHAS | Optional[int] | |
| `guru` | GURU | Optional[int] | |
| `prasekolah` | PRASEKOLAH | Optional[bool] | ADA/TIADA |
| `integrasi` | INTEGRASI | Optional[bool] | ADA/TIADA |
| `koordinatXX` | KOORDINATXX | Optional[float] | |
| `koordinatYY` | KOORDINATYY | Optional[float] | |
| `skmLEQ150` | SKM<=150 | Optional[bool] | YA → True |


## License
TBD

