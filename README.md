# sekolahku-mod-dataproc


```bash
# Run the ingestion pipeline
python -m src.main

# Inline override example
# For source: csv
SOURCE=csv python -m src.main

# For source: Google Sheets
SOURCE=gsheet GSHEET_ID=<ID> GSHEET_WORKSHEET_NAME=<worksheet> \
	GOOGLE_APPLICATION_CREDENTIALS=docs/service_account.json python -m src.main

# Run ingestion and EntitiSekolah aggregation
SOURCE=csv python -m src.main --entiti

# Show verbosity for a single run (flag expects a value)
python -m src.main --log-level DEBUG
```

### Command-line flags

Configuration (source, paths, Mongo connection, etc.) is controlled entirely through environment variables. Define values in `.env` or export them before running the module. The CLI exposes a couple of runtime toggles:

- `--entiti` triggers the EntitiSekolah aggregation pipeline.
- `--log-level <LEVEL>` adjusts logging verbosity for the current process (choose from `DEBUG`, `INFO`, `WARNING`, `ERROR`).

The entiti flag writes to the `EntitiSekolah` collection.

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

# Run EntitiSekolah pipeline
python -m src.main --entiti

# Run AnalitikSekolah pipeline
python -m src.main --analitik

# Increase verbosity for a single run
python -m src.main --log-level DEBUG
```

The process reads configuration from environment variables.

Each run prints a summary dictionary, for example :

```bash
python -m src.main
```
`Ingestion summary: {'collection': 'Sekolah', 'total': 10245, 'processed': 10244, 'failed': 1, 'errors': [{'row': 1, 'error': 'kodSekolah is required'}], 'inserted': 10244, 'dry_run': False}`

```bash
python -m src.main --entiti
```
`Entiti summary: {'entiti': {'collection': 'EntitiSekolah', 'total': 10244, 'processed': 10244, 'failed': 0, 'errors': [], 'inserted': 10244, 'dry_run': False}}`. 

```bash
python -m src.main --analitik
```
`Analitik summary: {'analitik': {'processed': 1, 'inserted': 1, 'dry_run': False, 'collection': 'AnalitikSekolah'}}`


## Data Model

### Sekolah (source dataset)

| Field | Source Column | Type | Notes |
|-------|---------------|------|-------|
| `negeri` | NEGERI | Optional[str] | |
| `ppd` | PPD | Optional[str] | |
| `parlimen` | PARLIMEN | Optional[str] | |
| `dun` | DUN | Optional[str] | |
| `peringkat` | PERINGKAT | Optional[str] | |
| `jenisLabel` | JENIS/LABEL | Optional[str] | |
| `kodSekolah` | KODSEKOLAH | str | School code |
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

### EntitiSekolah (aggregation output)

Each aggregated document is stored in the `EntitiSekolah` collection with the shape below.

`data` currently groups derived attributes into the following sub-documents:

- `infoSekolah`
- `infoKomunikasi`
- `infoPentadbiran`
- `infoLokasi`

| Field | Type | Notes |
|-------|------|-------|
| `namaSekolah` | Optional[str] | Mirrors `Sekolah.namaSekolah` |
| `kodSekolah` | str | Primary identifier (matches source `kodSekolah`) |
| `data` | object | Structured aggregates and derived attributes (see tables below) |
| `updatedAt` | datetime | UTC timestamp when the entity snapshot was generated |

#### `data.infoSekolah`

| Field | Type | Notes |
|-------|------|-------|
| `jenisLabel` | Optional[str] | School type/label carried over from source |
| `jumlahPelajar` | Optional[int] | Sum of `enrolmenPrasekolah + enrolmen + enrolmenKhas` |
| `jumlahGuru` | Optional[int] | Mirrors source `guru` |

#### `data.infoKomunikasi`

| Field | Type | Notes |
|-------|------|-------|
| `noTelefon` | Optional[str] | Primary contact number |
| `noFax` | Optional[str] | Fax number |
| `email` | Optional[EmailStr] | General contact email |
| `alamatSurat` | Optional[str] | Mailing address |
| `poskodSurat` | Optional[str] | Postal code (stored as string) |
| `bandarSurat` | Optional[str] | Mailing city |

#### `data.infoPentadbiran`

| Field | Type | Notes |
|-------|------|-------|
| `negeri` | Optional[str] | State |
| `ppd` | Optional[str] | District education office |
| `parlimen` | Optional[str] | Parliament constituency |
| `bantuan` | Optional[str] | Assistance classification |
| `bilSesi` | Optional[str] | Number of sessions |
| `sesi` | Optional[str] | Session descriptor |
| `prasekolah` | Optional[bool] | Indicates preschool programme |
| `integrasi` | Optional[bool] | Indicates integration programme |

#### `data.infoLokasi`

| Field | Type | Notes |
|-------|------|-------|
| `koordinatXX` | Optional[float] | Longitude |
| `koordinatYY` | Optional[float] | Latitude |
| `location` | Optional[object] | GeoJSON `Point` with `[longitude, latitude]` |

### AnalitikSekolah (aggregation output)

Each aggregated document is stored in the `AnalitikSekolah` collection with the shape below.

| Field | Type | Notes |
|-------|------|-------|
| `jumlahSekolah` | int | Total number of `sekolah` in the dataset |
| `jumlahGuru` | int | Total number of `guru` across all schools |
| `jumlahPelajar` | int | Total number of `pelajar` across all schools |
| `data` | object | Structured analytics and statistics (see tables below) |
| `updatedAt` | datetime | UTC timestamp when the analytics snapshot was generated |

#### `data.jenisLabel`

Array of school type statistics, each containing:

| Field | Type | Notes |
|-------|------|-------|
| `jenis` | str | School type/label (e.g., "SK", "SMK", "SJKC", "SJKT") |
| `peratus` | float | Percentage of total `sekolah` |
| `total` | int | Absolute count of `sekolah` of this type |

#### `data.bantuan`

Array of assistance/funding type statistics, each containing:

| Field | Type | Notes |
|-------|------|-------|
| `jenis` | str | `Bantuan` type (e.g., "SK", "SBK") |
| `peratus` | float | Percentage of total `sekolah` |
| `total` | int | Absolute count of `sekolah` with this assistance type |


## License
TBD

