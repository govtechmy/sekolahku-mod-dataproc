# sekolahku-mo# Run ingestion and then statistik (writes Stati# Run ingestion and statistik pipeline
python -m src.main --statistik* collections)
SOURCE=csv python -m src.main --statistikdataproc

```bash
# Run the ingestion pipeline
python -m src.main

# Inline override example
# For source: csv
SOURCE=csv python -m src.main

# For source: Google Sheets
SOURCE=gsheet GSHEET_ID=<ID> GSHEET_WORKSHEET_NAME=<worksheet> \
	GOOGLE_APPLICATION_CREDENTIALS=docs/service_account.json python -m src.main

# Run ingestion and then statistics (writes Statistik* collections)
SOURCE=csv python -m src.main --statistik

# Run ingestion and EntitiSekolah aggregation
SOURCE=csv python -m src.main --entiti

# Show verbosity for a single run (flag expects a value)
python -m src.main --log-level DEBUG
```

### Command-line flags

Configuration (source, paths, Mongo connection, etc.) is controlled entirely through environment variables. Define values in `.env` or export them before running the module. The CLI exposes a couple of runtime toggles:

- `--statistik` triggers the post-ingestion statistik run.
- `--entiti` triggers the EntitiSekolah aggregation pipeline.
- `--log-level <LEVEL>` adjusts logging verbosity for the current process (choose from `DEBUG`, `INFO`, `WARNING`, `ERROR`).

When the statistik flag is used, the pipeline rewrites the three Statistik collections. The entiti flag writes to the `EntitiSekolah` collection.

## Requirements

Python 3.11+

Install dependencies:
```bash
pip install -r requirements.txt
```

## Environment Variables

Refer to .env.example.
Create a `.env`.

If your Mongo instance requires authentication, embed the `username:password@` portion and any `authSource` query params directly in `MONGO_URI`.

## Running Ingestion

Set the required variables through `.env` or inline exports, then launch the module:

```bash
python -m src.main

# Inline override example
SOURCE=gsheet GSHEET_ID=<ID> GSHEET_WORKSHEET_NAME=<worksheet> \
	GOOGLE_APPLICATION_CREDENTIALS=docs/service_account.json python -m src.main

# Run ingestion and statistics pipeline
python -m src.main --statistik

# Run ingestion and EntitiSekolah pipeline
python -m src.main --entiti

# Increase verbosity for a single run
python -m src.main --log-level DEBUG
```

The process reads configuration from environment variables. Only `--statistik`, `--entiti`, and `--log-level <LEVEL>` affect runtime behavior.

Each run prints a summary dictionary, for example `{"processed": 1234, "inserted": 1234, "dry_run": 0}`. In dry-run mode, `processed` still reflects the number of documents that would be written, while `inserted` remains `0`.

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

