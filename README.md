# sekolahku-mod-dataproc

Data processing pipeline for Sekolahku - handles ingestion, transformation, and aggregation of school data from various sources into MongoDB collections.

## Table of Contents

- [Requirements](#requirements)
- [Setup](#setup)
- [Running the Application](#running-the-application)
  - [CLI Usage](#cli-usage)
  - [API Server](#api-server)
- [API Endpoints](#api-endpoints)
- [MongoDB Setup](#mongodb-setup)

---

## Requirements

- Python 3.11+
- MongoDB instance (local or remote)
- AWS S3 access (for polygon data and assets)

## Setup

### 1. Clone and Navigate

```bash
git clone <repository-url>
cd sekolahku-mod-dataproc
```

### 2. Create Virtual Environment

```bash
python -m venv venv
source venv/bin/activate  # On Windows: venv\Scripts\activate
```

### 3. Install Dependencies

```bash
pip install -r requirements.txt
```

### 4. Configure Environment Variables

Create a `.env` file in the project root (refer to `.env.example` for reference):

```bash
# MongoDB Configuration
MONGO_URI=mongodb://localhost:27017
DB_NAME=sekolahku

# AWS S3 Configuration
AWS_ACCESS_KEY_ID=your_access_key
AWS_SECRET_ACCESS_KEY=your_secret_key
S3_BUCKET_DATAPROC=your-dataproc-bucket
S3_BUCKET_PUBLIC=your-public-bucket

# CSV Data Path
CSV_PATH=data/sekolah.csv

# Asset Configuration (optional)
ASSET_LOGO_SEKOLAH_CSV=assets/logo.csv
```

**Note:** If your MongoDB requires authentication, embed credentials in the `MONGO_URI`:
```
MONGO_URI=mongodb://username:password@host:port/?authSource=admin
```

---

## Running the Application

### CLI Usage

The CLI provides various commands for data ingestion and processing:

#### Basic Ingestion

```bash
# Run full ingestion pipeline
python -m src.main

# Use a different CSV file
CSV_PATH=data/custom_sekolah.csv python -m src.main

# Enable debug logging
python -m src.main --log-level DEBUG
```

#### Aggregation Pipelines

```bash
# Run EntitiSekolah aggregation
python -m src.main --entiti

# Run AnalitikSekolah aggregation
python -m src.main --analitik
```

#### Polygon Data Processing

```bash
# Extract polygons from OpenDOSM to S3
python -m src.service.polygons.scrape_opendosm_negeri
python -m src.service.polygons.scrape_opendosm_parlimen

# Load polygons from S3 into MongoDB
python -m src.main --load-polygons
```

#### Command-line Flags

- `--entiti` - Trigger EntitiSekolah aggregation pipeline
- `--analitik` - Trigger AnalitikSekolah aggregation pipeline
- `--load-polygons` - Load OpenDOSM polygon data from S3 into MongoDB
- `--log-level <LEVEL>` - Set logging level (`DEBUG`, `INFO`, `WARNING`, `ERROR`)

---

### API Server

Start the FastAPI server using uvicorn:

```bash
uvicorn src.api:app --reload

# Run on a specific host and port
uvicorn src.api:app --host 0.0.0.0 --port 8000
```

The API will be available at `http://localhost:8000`

#### Scheduled Jobs

The API includes an automated daily ingestion job:
- **Schedule:** Daily at midnight (`0 0 * * *`)
- **Function:** Executes full ingestion pipeline (CSV ingestion, EntitiSekolah, NegeriParlimenKodSekolah, AnalitikSekolah)

To modify the schedule, edit the cron expression in [src/api.py](src/api.py):
```python
@crons.cron("0 2 * * *")  # Example: daily at 2 AM
```

---

## API Endpoints

### Health Check

**`GET /health`**

Verifies MongoDB connectivity and API health.

**Response:**
```json
{
  "status": "ok",
  "database": "sekolahku"
}
```

Returns `503 Service Unavailable` if database is unreachable.

---

### Data Ingestion

**`POST /load-full-ingestion`**

Triggers the complete data ingestion pipeline on-demand.

**Usage:**
```bash
curl -X POST http://localhost:8000/load-full-ingestion
```

**Monitoring:** Check the server terminal for detailed logs:
- `"Received request to trigger full ingestion"` - Job started
- `"Manual ingestion job completed successfully"` - Job succeeded
- Error messages with stack traces for troubleshooting

---

### Polygon Data

**`POST /scrape-opendosm-negeri-parlimen-polygons`**

Scrapes OpenDOSM polygon data (Negeri and Parlimen) and uploads to S3.

**Usage:**
```bash
curl -X POST http://localhost:8000/scrape-opendosm-negeri-parlimen-polygons
```

Downloads GeoJSON files from OpenDOSM and stores them in S3 for later processing. Runs as a background task.

---

### Asset Management

**`POST /export-asset-logo`**

Processes base64-encoded logo images from CSV and uploads to S3.

**Behavior:**
- Reads CSV with base64-encoded logo images
- Processes **all schools** in MongoDB `Sekolah` collection
- Schools in CSV: Logo uploaded if base64 data exists
- Schools NOT in CSV: Asset record created with `logo: null`
- Uploads to S3 at: `{negeri}/{parlimen}/{kodSekolah}/assets/logo.{ext}`
- Stores URLs in `AssetSekolah` collection

**Usage:**
```bash
curl -X POST http://localhost:8000/export-asset-logo
```

**Response:**
```json
{"status": "received"}
```

**Monitoring:** Check server logs for processing summary:
```
CSV asset processing completed: uploaded=9253 skipped=70 failed=0
```

**Output Manifest:** A detailed manifest is generated at `s3://your-public-bucket/manifest.json` containing:
```json
{
  "generatedAt": "2026-01-05T10:30:00.000Z",
  "totalSekolah": 10244,
  "sekolah": [
    {
      "kodSekolah": "WBA0031",
      "negeri": "PERAK",
      "parlimen": "TAPAH",
      "logoStatus": "UPLOADED",
      "logoUrl": "https://...logo.jpg"
    }
  ]
}
```

---

### School Entity Revalidation

**`GET /revalidate-school-entity`**

Triggers revalidation of school entities and exports to S3.

---

### Static JSON Generation

**`POST /generate-snap-routes`**

Generates `common/snap-routes.json` containing route mappings for all schools.

**Output Structure:**
```json
[
  "/",
  "/home",
  "/carian-sekolah",
  "/halaman-sekolah/AAA0001",
  "/halaman-sekolah/AAA0002"
]
```

---

**`POST /generate-school-list`**

Generates `common/school-list.json` containing categorized lists of schools.

**Output Structure:**
```json
[
  {
    "KODSEKOLAH": "AAA0001",
    "NAMASEKOLAH": "Sekolah Kebangsaan Example"
  }
]
```

Both endpoints upload generated files to the configured S3 bucket for frontend consumption.

---

## MongoDB Setup

### Creating Indexes

MongoDB indexes are managed in [src/db/indexes.py](src/db/indexes.py).

Run once to create all required indexes:

```bash
python -m src.db.indexes
```

Running multiple times is safe - existing indexes will be skipped.

### Collections

The pipeline creates and manages the following MongoDB collections:

- **`Sekolah`** - Raw school data from CSV ingestion
- **`EntitiSekolah`** - Aggregated school entities with structured data
- **`AnalitikSekolah`** - Statistical analytics across all schools
- **`AssetSekolah`** - S3 URLs for school assets (logos, images)
- **`NegeriPolygon`** - State boundary GeoJSON polygons
- **`ParlimenPolygon`** - Parliamentary constituency boundary GeoJSON polygons
- **`NegeriParlimenKodSekolah`** - Lookup table for school code mappings

---
