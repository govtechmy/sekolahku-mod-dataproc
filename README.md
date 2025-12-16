# sekolahku-mod-dataproc

```bash
# Run the ingestion pipeline
python -m src.main

# Inline override example
# Point to a different CSV file for one-off runs
CSV_PATH=data/custom_sekolah.csv python -m src.main

# Run ingestion and EntitiSekolah aggregation
python -m src.main --entiti

# Extract GeoJSON polygons from OPENDOSM to S3
python -m src.service.polygons.scrape_opendosm_negeri
python -m src.service.polygons.scrape_opendosm_parlimen

# Load extracted polygons from S3 into MongoDB
python -m src.main --load-polygons

# Export school assets to public S3 bucket
python -m src.main --export-assets
python -m src.main --export-assets --asset-status-filter "ACTIVE"

# Show verbosity for a single run
python -m src.main --log-level DEBUG
```

### Command-line flags

Configuration (source, paths, Mongo connection, etc.) is controlled entirely through environment variables. Define values in `.env` or export them before running the module. The CLI exposes a couple of runtime toggles:

- `--entiti` triggers the EntitiSekolah aggregation pipeline.
- `--analitik` triggers the AnalitikSekolah aggregation pipeline.
- `--load-polygons` loads OpenDOSM polygon seed data from S3 into `NegeriPolygon` and `ParlimenPolygon` collections
- `--log-level <LEVEL>` adjusts logging verbosity for the current process (choose from `DEBUG`, `INFO`, `WARNING`, `ERROR`).

The `--entiti` flag writes to the `EntitiSekolah` collection. The `--analitik` flag writes to the `AnalitikSekolah` collection. The `--load-polygons` flag reads from S3 and writes to the `NegeriPolygon` and `ParlimenPolygon` collections.

### Managing MongoDB Indexes

MongoDB indexes for this project is managed in `src/db/indexes.py`.

These indexes only need to be created once for your database.
If you need to rerun the setup, simply execute:

```bash
python -m src.db.indexes
```

Running the command multiple times will skip any existing indexes with matching key definitions.

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

# Load polygon seed collections from S3
python -m src.main --load-polygons

# Increase verbosity for a single run
python -m src.main --log-level DEBUG
```

The process reads configuration from environment variables.

Each run prints a summary dictionary, for example:

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

```bash
python -m src.main --load-polygons
```

`Negeri summary: {'negeri': {'processed': 16, 'succeeded': 16, 'failed': 0, 'collection': 'NegeriPolygon'}, 'total_negeri_files_scanned': 16}`

`Parlimen summary: {'parlimen': {'processed': 222, 'succeeded': 222, 'failed': 0, 'skipped': 0, 'collection': 'ParlimenPolygon'}, 'total_files_scanned': 222}`

```bash
python -m src.service.polygons.scrape_opendosm_negeri

python -m src.service.polygons.scrape_opendosm_parlimen
```

```
Extraction summary: {'extracted': 16, 'uploaded_to_s3': 16}
Extraction summary: {'extracted': 222, 'uploaded_to_s3': 222}
```

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

- **`POST /export-school-assets`** - Export school assets (logo, hero, gallery) to public S3 bucket
  
  - Copies assets from source bucket to target bucket with structure: `negeri/parliament/sekolah_kod/assets/`
  - Query parameter: `status_filter` (default: "ACTIVE")
  - Generates manifest file listing exported schools and missing assets
  - See [ASSET_EXPORT_GUIDE.md](ASSET_EXPORT_GUIDE.md) for detailed documentation

### Scheduled Cron Jobs

The FastAPI service includes **automated daily ingestion** via `fastapi-crons`:

- **Schedule**: `0 0 * * *` (daily at midnight)
- **Timezone**: Runs in server's local timezone
- **Function**: Executes the full ingestion pipeline (CSV ingestion, EntitiSekolah, NegeriParlimenKodSekolah, AnalitikSekolah)

**To adjust the schedule**, modify the cron expression in `src/api.py`:

```python
@crons.cron("0 2 * * *")  # Example: daily at 2 AM
```

**To disable**, comment out the `@crons.cron()` decorator or remove the cron startup in `src/api.py`.

#### Monitoring Scheduled Jobs

The cron job produces detailed logs with start/end times, duration, and metrics for each pipeline stage (ingestion, EntitiSekolah, NegeriParlimenKodSekolah, AnalitikSekolah).

**Important**: Failed jobs are logged but do **not** crash the server. The job retries at the next scheduled time.

#### Manual vs Scheduled Ingestion

- **Scheduled**: Automatic at 00:00 daily for regular updates
- **Manual**: `POST /trigger-ingestion` for on-demand runs, testing, or recovery

Both execute the same pipeline, log comprehensive metrics, and handle errors gracefully.

## Data Model

### Sekolah (source dataset)

| Field                | Source Column       | Type               | Notes          |
| -------------------- | ------------------- | ------------------ | -------------- |
| `negeri`             | NEGERI              | Optional[str]      |                |
| `ppd`                | PPD                 | Optional[str]      |                |
| `parlimen`           | PARLIMEN            | Optional[str]      |                |
| `dun`                | DUN                 | Optional[str]      |                |
| `peringkat`          | PERINGKAT           | Optional[str]      |                |
| `jenisLabel`         | JENIS/LABEL         | Optional[str]      |                |
| `kodSekolah`         | KODSEKOLAH          | str                | School code    |
| `namaSekolah`        | NAMASEKOLAH         | Optional[str]      |                |
| `alamatSurat`        | ALAMATSURAT         | Optional[str]      |                |
| `poskodSurat`        | POSKODSURAT         | Optional[int]      |                |
| `bandarSurat`        | BANDARSURAT         | Optional[str]      |                |
| `noTelefon`          | NOTELEFON           | Optional[str]      | "TIADA" → None |
| `noFax`              | NOFAX               | Optional[str]      | "TIADA" → None |
| `email`              | EMAIL               | Optional[EmailStr] | Normalized     |
| `lokasi`             | LOKASI              | Optional[str]      |                |
| `gred`               | GRED                | Optional[str]      |                |
| `bantuan`            | BANTUAN             | Optional[str]      |                |
| `bilSesi`            | BILSESI             | Optional[str]      |                |
| `sesi`               | SESI                | Optional[str]      |                |
| `enrolmenPrasekolah` | ENROLMEN PRASEKOLAH | Optional[int]      |                |
| `enrolmen`           | ENROLMEN            | Optional[int]      |                |
| `enrolmenKhas`       | ENROLMEN KHAS       | Optional[int]      |                |
| `guru`               | GURU                | Optional[int]      |                |
| `prasekolah`         | PRASEKOLAH          | Optional[bool]     | ADA/TIADA      |
| `integrasi`          | INTEGRASI           | Optional[bool]     | ADA/TIADA      |
| `koordinatXX`        | KOORDINATXX         | Optional[float]    |                |
| `koordinatYY`        | KOORDINATYY         | Optional[float]    |                |
| `skmLEQ150`          | SKM<=150            | Optional[bool]     | YA → True      |

### EntitiSekolah (aggregation output)

Each aggregated document is stored in the `EntitiSekolah` collection with the shape below.

`data` currently groups derived attributes into the following sub-documents:

- `infoSekolah`
- `infoKomunikasi`
- `infoPentadbiran`
- `infoLokasi`

| Field         | Type          | Notes                                                           |
| ------------- | ------------- | --------------------------------------------------------------- |
| `namaSekolah` | Optional[str] | Mirrors `Sekolah.namaSekolah`                                   |
| `kodSekolah`  | str           | Primary identifier (matches source `kodSekolah`)                |
| `data`        | object        | Structured aggregates and derived attributes (see tables below) |
| `updatedAt`   | datetime      | UTC timestamp when the entity snapshot was generated            |

#### `data.infoSekolah`

| Field           | Type          | Notes                                                 |
| --------------- | ------------- | ----------------------------------------------------- |
| `jenisLabel`    | Optional[str] | School type/label carried over from source            |
| `jumlahPelajar` | Optional[int] | Sum of `enrolmenPrasekolah + enrolmen + enrolmenKhas` |
| `jumlahGuru`    | Optional[int] | Mirrors source `guru`                                 |

#### `data.infoKomunikasi`

| Field         | Type               | Notes                          |
| ------------- | ------------------ | ------------------------------ |
| `noTelefon`   | Optional[str]      | Primary contact number         |
| `noFax`       | Optional[str]      | Fax number                     |
| `email`       | Optional[EmailStr] | General contact email          |
| `alamatSurat` | Optional[str]      | Mailing address                |
| `poskodSurat` | Optional[str]      | Postal code (stored as string) |
| `bandarSurat` | Optional[str]      | Mailing city                   |

#### `data.infoPentadbiran`

| Field        | Type           | Notes                           |
| ------------ | -------------- | ------------------------------- |
| `negeri`     | Optional[str]  | State                           |
| `ppd`        | Optional[str]  | District education office       |
| `parlimen`   | Optional[str]  | Parliament constituency         |
| `bantuan`    | Optional[str]  | Assistance classification       |
| `bilSesi`    | Optional[str]  | Number of sessions              |
| `sesi`       | Optional[str]  | Session descriptor              |
| `prasekolah` | Optional[bool] | Indicates preschool programme   |
| `integrasi`  | Optional[bool] | Indicates integration programme |

#### `data.infoLokasi`

| Field         | Type             | Notes                                        |
| ------------- | ---------------- | -------------------------------------------- |
| `koordinatXX` | Optional[float]  | Longitude                                    |
| `koordinatYY` | Optional[float]  | Latitude                                     |
| `location`    | Optional[object] | GeoJSON `Point` with `[longitude, latitude]` |

### AnalitikSekolah (aggregation output)

Each aggregated document is stored in the `AnalitikSekolah` collection with the shape below.

| Field           | Type     | Notes                                                   |
| --------------- | -------- | ------------------------------------------------------- |
| `jumlahSekolah` | int      | Total number of `sekolah` in the dataset                |
| `jumlahGuru`    | int      | Total number of `guru` across all schools               |
| `jumlahPelajar` | int      | Total number of `pelajar` across all schools            |
| `data`          | object   | Structured analytics and statistics (see tables below)  |
| `updatedAt`     | datetime | UTC timestamp when the analytics snapshot was generated |

#### `data.jenisLabel`

Array of school type statistics, each containing:

| Field     | Type  | Notes                                                 |
| --------- | ----- | ----------------------------------------------------- |
| `jenis`   | str   | School type/label (e.g., "SK", "SMK", "SJKC", "SJKT") |
| `peratus` | float | Percentage of total `sekolah`                         |
| `total`   | int   | Absolute count of `sekolah` of this type              |

#### `data.bantuan`

Array of assistance/funding type statistics, each containing:

| Field     | Type  | Notes                                                 |
| --------- | ----- | ----------------------------------------------------- |
| `jenis`   | str   | `Bantuan` type (e.g., "SK", "SBK")                    |
| `peratus` | float | Percentage of total `sekolah`                         |
| `total`   | int   | Absolute count of `sekolah` with this assistance type |

---

## Polygon Seed Collections

The polygon loading pipeline extracts GeoJSON boundary data from OpenDOSM and populates MongoDB collections.

### Workflow

1. **Extract**: Download polygons from Kawasanku API and upload to S3

   ```bash
   python -m src.service.polygons.scrape_opendosm_negeri
   python -m src.service.polygons.scrape_opendosm_parlimen
   ```

2. **Load**: Read from S3 and insert into MongoDB
   ```bash
   python -m src.main --load-polygons
   # OR via API endpoint
   curl -X POST http://localhost:8000/load-opendosm-polygons
   ```

### NegeriPolygon

Each document represents a Negeri boundary..

| Field       | Type     | Notes                                                                 |
| ----------- | -------- | --------------------------------------------------------------------- |
| `_id`       | str      | Negeri identifier (e.g., `JOHOR`, `WILAYAH_PERSEKUTUAN_KUALA_LUMPUR`) |
| `negeri`    | str      | Negeri name matching `NegeriEnum`                                     |
| `geometry`  | object   | GeoJSON `MultiPolygon` with state boundary coordinates                |
| `updatedAt` | datetime | UTC timestamp when the polygon data was loaded                        |

**Data source:** `s3://{S3_BUCKET_DATAPROC}/opendosm/raw/negeri/`

### ParlimenPolygon

Each document represents a parliamentary boundary.

| Field       | Type     | Notes                                                            |
| ----------- | -------- | ---------------------------------------------------------------- |
| `_id`       | str      | Composite key: `"{negeri}::{parlimen}"` (e.g., `JOHOR::SEGAMAT`) |
| `negeri`    | str      | Negeri name matching `NegeriEnum`                                |
| `parlimen`  | str      | Parliamentary name                                               |
| `geometry`  | object   | GeoJSON `MultiPolygon` with constituency boundary coordinates    |
| `updatedAt` | datetime | UTC timestamp when the polygon data was loaded                   |

**Data source:** `s3://{S3_BUCKET_DATAPROC}/opendosm/raw/parlimen/`

## Dataproc API

The Dataproc API provides endpoints for generating static JSON files used by the Sekolahku web application on demand.

### Generate Snap Routes JSON

**Endpoint:** `POST /generate-snap-routes`

Generates the snap routes JSON file containing route mappings for all schools and uploads it to S3. This file is used by the frontend to provide quick navigation and school-specific routing.

#### Generated Output

The endpoint creates `common/snap-routes.json` in S3 with the following structure:

```json
[
  "/",
  "/home",
  "/about",
  "/carian-sekolah",
  "/siaran",
  "/halaman-sekolah/AAA0001",
  "/halaman-sekolah/AAA0002",
  ...
]
```

### Generate School Lists JSON

**Endpoint:** `POST /generate-school-list`

Generates the school lists JSON file containing categorized lists of schools (by negeri, parlimen, etc.) and uploads it to S3. This file is used by the frontend for school discovery and filtering.

#### Generated Output

The endpoint creates `common/school-list.json` in S3 with the following structure:

```json
[
  {
    "KODSEKOLAH": "AAA0001",
    "NAMASEKOLAH": "Sekolah Kebangsaan Example"
  },
  {
    "KODSEKOLAH": "AAA0002",
    "NAMASEKOLAH": "Sekolah Menengah Example"
  },
  ...
]
```

**Response:**

- `200 OK` - Successfully generated and uploaded school lists JSON
- Returns a success message with the S3 path where the file was stored

**Note:** Both endpoints trigger on-demand generation and upload to the configured S3 bucket. The generated files are immediately available for consumption by the web application.

---

## License

TBD
