from __future__ import annotations

import logging
from typing import Any

from botocore.exceptions import ClientError
from fastapi import BackgroundTasks, FastAPI, HTTPException
from fastapi_crons import Crons
from pydantic import BaseModel
from pymongo import MongoClient
from pymongo.errors import PyMongoError

from src.main import run_ingest
from src.config.settings import get_settings
from src.service.entiti_revalidate import revalidate_school_entity
from src.service.polygons import load_opendosm_negeri, load_opendosm_parlimen
from src.service.polygons import scrape_opendosm_negeri, scrape_opendosm_parlimen
from src.service.exporters.export_polygons import export_all_polygons
from src.service.builders.build_snap_routes import generate_and_upload_snap_routes
from src.service.builders.build_school_list import generate_and_upload_school_list
from src.pipeline.malaysia_polygon import run_malaysia_polygon_pipeline
from src.service.assets import process_csv_assets

logger = logging.getLogger(__name__)
app = FastAPI()
crons = Crons()

settings = get_settings()


# -----------------------------------------------------------------------------
# Request Models
# -----------------------------------------------------------------------------

class ProcessCsvAssetsRequest(BaseModel):
    csv_path: str

if not logging.getLogger().handlers:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
    )

@app.post("/generate-snap-routes", tags=["s3-publisher"])
def generate_snap_routes_endpoint(background_tasks: BackgroundTasks) -> dict[str, str | int]:
    """Generate snap-routes.json and upload to S3."""
    try:
        count = generate_and_upload_snap_routes()
        return {"status": "received", "count": count}
    except PyMongoError as e:
        logger.exception("Database error while generating snap-routes: %s", e)
        raise HTTPException(status_code=500, detail="Database error while generating snap-routes")
    except ClientError as e:
        logger.exception("Failed uploading snap-routes.json to S3: %s", e)
        raise HTTPException(status_code=502, detail="S3 upload failed while generating snap-routes")
    except Exception as e:
        logger.exception("Unexpected error while generating snap-routes: %s", e)
        raise HTTPException(status_code=500, detail="Unexpected error while generating snap-routes")

@app.post("/generate-school-list", tags=["s3-publisher"])
def generate_school_list_endpoint(background_tasks: BackgroundTasks) -> dict[str, str | int]:
    """Generate school-list.json and upload to S3."""
    try:
        count = generate_and_upload_school_list()
        return {"status": "received", "count": count}
    except PyMongoError as e:
        logger.exception("Database error while generating school list: %s", e)
        raise HTTPException(status_code=500, detail="Database error while generating school list")
    except ClientError as e:
        logger.exception("Failed uploading school-list.json to S3: %s", e)
        raise HTTPException(status_code=502, detail="S3 upload failed while generating school list")
    except Exception as e:
        logger.exception("Unexpected error while generating school list: %s", e)
        raise HTTPException(status_code=500, detail="Unexpected error while generating school list")

def _run_revalidate_school_entity_job(settings: Any) -> None:
    """Execute school entity revalidation and log outcome."""
    try:
        summary = revalidate_school_entity(settings)
    except PyMongoError:
        logger.exception("MongoDB error while handling revalidation request")
        return
    except ClientError:
        logger.exception("S3 error while handling revalidation request")
        return

    logger.info(
        "Revalidation completed successfully: bucket=%s processed=%s",
        summary.get("bucket"),
        summary.get("processed"),
    )

def _run_ingestion_job() -> None:
    """Execute ingestion pipeline and log outcome."""
    try:
        run_ingest()
        logger.info("Manual ingestion job completed successfully")
    except PyMongoError:
        logger.exception("MongoDB error while handling ingestion request")
    except Exception:
        logger.exception("Unexpected error while handling ingestion request")

@crons.cron("0 0 * * *")
async def daily_ingestion_job():
    """
    Run the full ingestion pipeline daily at midnight (00:00).
    
    This cron job executes the complete data ingestion process including:
    - Main school data ingestion
    - EntitiSekolah aggregation
    - NegeriParlimenKodSekolah population
    - Analitik aggregation (if data changed)
    """
    logger.info("Starting scheduled daily ingestion job")
    
    try:
        run_ingest()
        logger.info("Scheduled daily ingestion job completed successfully")
    except PyMongoError as exc:
        logger.error("Scheduled ingestion job failed - database error: %s", str(exc))
        logger.exception("Full database error details:")
        # DO NOT re-raise - allow the server to continue running
    except Exception as exc:
        logger.error("Scheduled ingestion job failed - unexpected error: %s", str(exc))
        logger.exception("Full error details:")
        # DO NOT re-raise - allow the server to continue running


@app.get("/health", tags=["health"])
def health_check() -> dict[str, str]:
    """Return application health status by verifying database connectivity."""
    client = MongoClient(settings.mongo_uri, serverSelectionTimeoutMS=2000)
    try:
        client.admin.command("ping")
    except PyMongoError as exc:
        raise HTTPException(status_code=503, detail="Database unreachable") from exc
    finally:
        client.close()
    return {"status": "ok", "database": settings.db_name}


@app.post("/load-full-ingestion", tags=["ingestion"])
def load_full_ingestion_endpoint(background_tasks: BackgroundTasks) -> dict[str, str]:
    """
    Trigger the full ingestion & processing pipeline from raw data source into MongoDB.
    The pipeline includes:
    - Sekolah raw data ingestion
    - EntitiSekolah aggregation
    - NegeriParlimenKodSekolah population
    - Analitik aggregation (if data changed)
    """
    logger.info("Received request to trigger full ingestion")
    
    background_tasks.add_task(_run_ingestion_job)
    
    return {"status": "received"}


@app.post("/revalidate-school-entity", tags=["s3-publisher"])
def revalidate_school_entity_endpoint(background_tasks: BackgroundTasks) -> dict[str, str]:
    """Trigger revalidation of school entities into S3 bucket."""

    logger.info("Received request to revalidate school entities")

    background_tasks.add_task(_run_revalidate_school_entity_job, settings)

    return {"status": "received"}


@app.post("/scrape-opendosm-negeri-parlimen-polygons", tags=["scraping"])
def scrape_opendosm_polygons_endpoint(background_tasks: BackgroundTasks) -> dict[str, str]:
    """Scrape OpenDOSM polygon data (Negeri and Parlimen) from source URLs and upload to S3.
    """
    
    def scrape_polygons_job():
        try:
            scrape_opendosm_negeri.main()
            logger.info("Negeri scraping completed successfully")
        except Exception as e:
            logger.exception("Error occurred while scraping Negeri data: %s", e)
        
        try:
            scrape_opendosm_parlimen.main()
            logger.info("Parlimen scraping completed successfully")
        except Exception as e:
            logger.exception("Error occurred while scraping Parlimen data: %s", e)

    background_tasks.add_task(scrape_polygons_job)

    return {"status": "received request to scrape OpenDOSM Negeri & Parlimen data"}


@app.post("/process-csv-assets", tags=["s3-publisher"])
def process_csv_assets_endpoint(
    payload: ProcessCsvAssetsRequest,
    background_tasks: BackgroundTasks,
) -> dict[str, str]:
    """
    Process sekolah assets from CSV file and upload to S3.
    
    Reads a CSV file containing base64-encoded images, validates and decodes them,
    uploads to S3 public bucket, and stores metadata in MongoDB Assets collection.
    
    Request Body:
        {
            "csv_path": "s3://bucket/key or local path"
        }
    
    Returns:
        Dictionary confirming that the processing job has been queued.
    """
    csv_path = payload.csv_path
    
    logger.info("Received request to process CSV assets from: %s", csv_path)
    
    def _run_csv_asset_processing_job():
        try:
            summary = process_csv_assets(settings, csv_path)
            logger.info(
                "CSV asset processing completed: uploaded=%s skipped=%s failed=%s",
                summary.get("uploaded"),
                summary.get("skipped"),
                summary.get("failed"),
            )
        except Exception:
            logger.exception("CSV asset processing failed")
    
    background_tasks.add_task(_run_csv_asset_processing_job)
    
    return {"status": "received", "csv_path": csv_path}


@app.post("/load-negeri-parlimen-polygons", tags=["ingestion"])
def load_negeri_parlimen_polygons_endpoint(background_tasks: BackgroundTasks) -> dict[str, str]:
    """Trigger the loading & processing of raw OpenDOSM Negeri & Parlimen polygons data from S3 into MongoDB.
    The pipeline includes:
    - centroid calculation
    """

    def load_polygons_sequentially():
        try:
            negeri= load_opendosm_negeri.main()
            parlimen = load_opendosm_parlimen.main()
            logger.info(f"Negeri summary: {negeri}")
            logger.info(f"Parlimen summary: {parlimen}")
        except Exception as e:
            logger.exception("Error occurred while loading polygons: %s", e)

    background_tasks.add_task(load_polygons_sequentially)

    return {"status": "received request to load Negeri & Parlimen polygons"}


@app.post("/export-polygons", tags=["s3-publisher"])
def export_polygons_endpoint(background_tasks: BackgroundTasks) -> dict[str, str]:
    """Trigger export of Negeri Polygons & Parlimen Polygons from MongoDB to S3 public bucket."""

    def export_polygons_job():
        try:
            summary = export_all_polygons()
            logger.info(f"Polygon export summary: {summary}")
        except Exception as e:
            logger.exception("Error occurred while exporting polygons: %s", e)

    background_tasks.add_task(export_polygons_job)

    return {"status": "received request to export polygons to S3"}


@app.on_event("startup")
async def startup_event():
    """Initialize and start scheduled cron jobs."""
    logger.info("Initializing scheduled cron jobs")
    await crons.start()
    logger.info("Cron jobs started successfully - daily ingestion scheduled for 00:00")


@app.on_event("shutdown")
async def shutdown_event():
    """Stop cron jobs on application shutdown."""
    logger.info("Stopping scheduled cron jobs")
    await crons.stop()
    logger.info("Cron jobs stopped")


@app.post("/load-malaysia-polygons", tags=["ingestion"])
def load_malaysia_polygons_endpoint() -> dict[str, str | int]:
    """Trigger the loading of MalaysiaPolygon collection.
    The pipeline includes:
    - the source data is from NegeriPolygon collection
    - merging all negeri polygons into a single malaysia polygon
    - calculating centroid
    """
    try:
        count = run_malaysia_polygon_pipeline()
        return { "status": "received request to load Malaysia Polygons", "count": count }
    except PyMongoError as e:
        logger.exception("Database error while loading Malaysia polygons: %s", e)
        raise HTTPException(status_code=500, detail="Database error while loading Malaysia polygons")
    except Exception as e:
        logger.exception("Unexpected error while loading Malaysia polygons: %s", e)
        raise HTTPException(status_code=500, detail="Unexpected error while loading Malaysia polygons")
