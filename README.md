# sekolahku-mod-dataproc


```bash
# Run the ingestion pipeline
python -m src.main

# Inline override example
# Point to a different CSV file for one-off runs
CSV_PATH=data/custom_sekolah.csv python -m src.main

# Run ingestion and EntitiSekolah aggregation
python -m src.main --entiti

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

### Environment setup

Create and activate a virtual environment, then install dependencies:

```bash
python -m venv venv
source venv/bin/activate  # On Windows use: venv\Scripts\activate
pip install -r requirements.txt
```

The dependency list includes FastAPI for the API health check. Ensure your environment exports the Mongo-related variables (`MONGO_URI`, `DB_NAME`, etc.) before running CLI or API commands.

## Environment Variables

Refer to .env.example.
Create a `.env`.

If your Mongo instance requires authentication, embed the `username:password@` portion and any `authSource` query params directly in `MONGO_URI`.

## Running Ingestion

Set the required variables through `.env` or inline exports, then launch the module:

```bash
python -m src.main

# Inline override example
CSV_PATH=data/custom_sekolah.csv python -m src.main

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
`Ingestion summary: {'collection': 'Sekolah', 'total': 10245, 'processed': 10244, 'failed': 1, 'errors': [{'row': 1, 'error': 'kodSekolah is required'}], 'inserted': 10244}`

```bash
python -m src.main --entiti
```
`Entiti summary: {'entiti': {'collection': 'EntitiSekolah', 'total': 10244, 'processed': 10244, 'failed': 0, 'errors': [], 'inserted': 10244}}`. 

```bash
python -m src.main --analitik
```
`Analitik summary: {'analitik': {'processed': 1, 'inserted': 1, 'collection': 'AnalitikSekolah'}}`


## Running the API

Serve the FastAPI application with an ASGI server such as `uvicorn` after activating your virtual environment:

```bash
uvicorn src.api:app --reload
```

### Available Endpoints

- **`GET /health`** - Health check that verifies MongoDB connectivity
  - Returns `{"status": "ok", "database": "<DB_NAME>"}` when database is reachable
  - Returns `503 Service Unavailable` if database is unreachable

- **`POST /trigger-ingestion`** - Manually trigger the full ingestion pipeline
  - Executes the complete data ingestion process on-demand
  - Returns detailed metrics and summary of all pipeline stages
  - Independent from the scheduled daily job

- **`GET /revalidate-school-entity`** - Trigger revalidation of school entities to S3

### Scheduled Cron Jobs

The FastAPI service includes **automated daily ingestion** via `fastapi-crons`:

#### Daily Ingestion Job
- **Schedule**: Runs every day at **00:00 (midnight)**
- **Timezone**: Configurable via `CRON_TIMEZONE` environment variable (default: `Asia/Kuala_Lumpur`)
- **Function**: Executes the full ingestion pipeline automatically:
  - Main school data ingestion from CSV
  - EntitiSekolah aggregation
  - NegeriParlimenKodSekolah population
  - AnalitikSekolah aggregation (if data changed)

#### Cron Configuration

The cron expression `"0 0 * * *"` means:
- `0` - Minute: 0 (on the hour)
- `0` - Hour: 0 (midnight)
- `*` - Day of month: every day
- `*` - Month: every month
- `*` - Day of week: every day of the week

**To adjust the schedule**, modify the cron expression in `src/api.py`:

```python
@crons.cron("0 0 * * *", tz=settings.cron_timezone)  # Daily at midnight
async def daily_ingestion_job():
    ...
```

Common schedule examples:
- `"0 */6 * * *"` - Every 6 hours
- `"0 2 * * *"` - Daily at 2:00 AM
- `"0 0 * * 0"` - Weekly on Sunday at midnight
- `"0 0 1 * *"` - Monthly on the 1st at midnight

**To change the timezone**, set the `CRON_TIMEZONE` environment variable:

```bash
# In .env file or environment
CRON_TIMEZONE=UTC
CRON_TIMEZONE=Asia/Singapore
CRON_TIMEZONE=America/New_York
```

**To disable the scheduled job**, comment out or remove the cron decorator in `src/api.py`:

```python
# @crons.cron("0 0 * * *", tz=settings.cron_timezone)
# async def daily_ingestion_job():
#     ...
```

Alternatively, don't start the cron scheduler by modifying the startup event.

#### Monitoring Scheduled Jobs

The cron job produces detailed logs for monitoring:

**Job Start:**
```
================================================================================
SCHEDULED INGESTION JOB STARTED
Start Time: 2025-12-08T00:00:00.123456
Timezone: Asia/Kuala_Lumpur
================================================================================
```

**Job Success:**
```
================================================================================
SCHEDULED INGESTION JOB COMPLETED SUCCESSFULLY
End Time: 2025-12-08T00:05:23.654321
Duration: 323.53 seconds
--------------------------------------------------------------------------------
INGESTION METRICS:
  - Total Processed: 10234
  - Inserted: 45
  - Updated: 123
  - Inactivated: 12
  - Failed: 0
  - Entiti Synced: 178
--------------------------------------------------------------------------------
[... additional metrics for EntitSekolah, NegeriParlimenKodSekolah, AnalitikSekolah ...]
================================================================================
```

**Job Failure:**
```
================================================================================
SCHEDULED INGESTION JOB FAILED - DATABASE ERROR
End Time: 2025-12-08T00:02:15.123456
Duration: 135.12 seconds
Error Type: ConnectionFailure
Error Message: Connection refused
================================================================================
Job failed but FastAPI server continues running
Next scheduled run: tomorrow at 00:00 Asia/Kuala_Lumpur
```

**Important**: Failed cron jobs are logged but do **not** crash the FastAPI server. The server continues running and the job will retry at the next scheduled time.

#### Manual vs Scheduled Ingestion

Both manual and scheduled ingestion methods are available:

| Method | Trigger | Use Case |
|--------|---------|----------|
| **Scheduled** | Automatic at 00:00 daily | Regular automated data updates |
| **Manual** | `POST /trigger-ingestion` | On-demand runs, testing, recovery |

Both methods:
- Execute the same complete pipeline
- Log comprehensive metrics
- Are independent and don't conflict
- Handle errors gracefully


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

