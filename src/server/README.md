# Sekolahku Dataproc API

On-demand JSON generation service for Sekolahku project. This API generates and uploads JSON files to S3 for consumption by the frontend application.

## Base URL

```
http://localhost:8002
```

## Authentication

All endpoints require API key authentication via the `x-api-key` header.

```
x-api-key: <your-api-key>
```

The API key is configured via the `DATAPROC_API_KEY` environment variable.

## Endpoints

### 1. Generate Snap Routes

**POST** `/dataproc/generate/snap-routes`

Generates a JSON file containing all routes for static site generation (SSG/ISR). The output includes fixed routes and dynamic school pages.

#### Headers

```
x-api-key: <your-api-key>
```

#### Response

```json
{
  "ok": true,
  "count": 10245
}
```

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

#### Process

1. Fetches all schools from the `entitisekolah` collection (only `_id` and `KODSEKOLAH` fields)
2. Generates routes for all fixed pages and school detail pages
3. Uploads the JSON to S3 bucket specified in `S3_BUCKET_DATAPROC`

#### Error Responses

- **401 Unauthorized**: Invalid or missing API key
- **500 Internal Server Error**: Database connection error or S3 upload failure

---

### 2. Generate School List

**POST** `/dataproc/generate/school-list`

Generates a JSON file containing a mapping of school codes to school names for search functionality and autocomplete features.

#### Headers

```
x-api-key: <your-api-key>
```

#### Response

```json
{
  "ok": true,
  "count": 10234
}
```

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

#### Process

1. Fetches all schools from the `entitisekolah` collection (only `_id`, `kodSekolah`, and `namaSekolah` fields)
2. Filters and maps school codes to names
3. Uploads the JSON to S3 bucket specified in `S3_BUCKET_DATAPROC`

#### Error Responses

- **401 Unauthorized**: Invalid or missing API key
- **500 Internal Server Error**: Database connection error or S3 upload failure

---

## Running the Server

### Development Mode

```bash
uvicorn src.server.run:app --reload --port 8002
```

### Production Mode

```bash
uvicorn src.server.run:app --host 0.0.0.0 --port 8002
```

## Environment Variables

Required environment variables:

```bash
DATAPROC_API_KEY=<your-secret-api-key>
S3_BUCKET_DATAPROC=<your-s3-bucket-name>
MONGO_URI=<your-mongodb-connection-string>
```

## Example Usage

### Using cURL

```bash
# Generate snap routes
curl -X POST http://localhost:8002/dataproc/generate/snap-routes \
  -H "x-api-key: your-api-key"

# Generate school list
curl -X POST http://localhost:8002/dataproc/generate/school-list \
  -H "x-api-key: your-api-key"
```

### Using Python

```python
import requests

API_KEY = "your-api-key"
BASE_URL = "http://localhost:8002"

headers = {"x-api-key": API_KEY}

# Generate snap routes
response = requests.post(f"{BASE_URL}/dataproc/generate/snap-routes", headers=headers)
print(response.json())

# Generate school list
response = requests.post(f"{BASE_URL}/dataproc/generate/school-list", headers=headers)
print(response.json())
```

### Using JavaScript/Fetch

```javascript
const API_KEY = "your-api-key";
const BASE_URL = "http://localhost:8002";

// Generate snap routes
fetch(`${BASE_URL}/dataproc/generate/snap-routes`, {
  method: "POST",
  headers: {
    "x-api-key": API_KEY,
  },
})
  .then((res) => res.json())
  .then((data) => console.log(data));

// Generate school list
fetch(`${BASE_URL}/dataproc/generate/school-list`, {
  method: "POST",
  headers: {
    "x-api-key": API_KEY,
  },
})
  .then((res) => res.json())
  .then((data) => console.log(data));
```

## API Documentation

Once the server is running, you can access the auto-generated API documentation:

- **Swagger UI**: http://localhost:8002/docs

## Notes

- Both endpoints are **POST** requests even though they don't accept request bodies
- The generated JSON files are uploaded to the S3 bucket under the `common/` prefix
- S3 keys are:
  - `common/snap-routes.json`
  - `common/school-list.json`
- The `count` in the response indicates the number of items in the generated JSON array
