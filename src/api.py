from __future__ import annotations

import logging
import os
from typing import Any, Optional

from botocore.exceptions import ClientError
from fastapi import BackgroundTasks, FastAPI, HTTPException
from fastapi import FastAPI, HTTPException
from fastapi import APIRouter, Header, status
from fastapi_crons import Crons
from pymongo import MongoClient
from pymongo.errors import PyMongoError

from src.config.settings import get_settings
from src.main import run_ingest
from src.service.entitiRevalidate import revalidate_school_entity
from src.service.polygons import load_opendosm_negeri, load_opendosm_parlimen

from src.core.db import get_entitisekolah_collection
from src.core.jsonhelpers import build_snap_routes, build_school_list
from src.core.s3 import upload_json_to_s3

if not logging.getLogger().handlers:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
    )

logger = logging.getLogger(__name__)
app = FastAPI()

router = APIRouter(prefix="/dataproc", tags=["dataproc"])

S3_PREFIX_SEKOLAH = "common"
SNAP_ROUTES_KEY = f"{S3_PREFIX_SEKOLAH}/snap-routes.json"
SCHOOL_LIST_KEY = f"{S3_PREFIX_SEKOLAH}/school-list.json"
S3_BUCKET = os.getenv("S3_BUCKET_DATAPROC") 

@router.post("/generate/snap-routes")
def generate_snap_routes():
    try:
        coll = get_entitisekolah_collection()
        docs = list(coll.find({}, {"_id": 1, "KODSEKOLAH": 1}))
    except PyMongoError:
        logger.exception("Failed reading DB")
        raise HTTPException(status_code=500, detail="Database error")

    payload = build_snap_routes(docs)

    try:
        upload_json_to_s3(payload, S3_BUCKET, SNAP_ROUTES_KEY)
    except Exception:
        logger.exception("Failed uploading snap-routes.json")
        raise HTTPException(status_code=500, detail="S3 upload error")

    return {"ok": True, "count": len(payload)}


@router.post("/generate/school-list")
def generate_school_list():
    try:
        coll = get_entitisekolah_collection()
        docs = list(coll.find({}, {"_id": 1, "kodSekolah": 1, "namaSekolah": 1}))
    except PyMongoError:
        logger.exception("Failed reading DB")
        raise HTTPException(status_code=500, detail="Database error")

    payload = build_school_list(docs)

    try:
        upload_json_to_s3(payload, S3_BUCKET, SCHOOL_LIST_KEY)
    except Exception:
        logger.exception("Failed uploading school-list.json")
        raise HTTPException(status_code=500, detail="S3 upload error")

    return {"ok": True, "count": len(payload)}

# Register all dataproc endpoints
app.include_router(router)

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

# Initialize settings to get timezone configuration
settings = get_settings()
crons = Crons()


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


@app.get("/health")
def health_check() -> dict[str, str]:
    """Return application health status by verifying database connectivity."""
    settings = get_settings()
    client = MongoClient(settings.mongo_uri, serverSelectionTimeoutMS=2000)
    try:
        client.admin.command("ping")
    except PyMongoError as exc:
        raise HTTPException(status_code=503, detail="Database unreachable") from exc
    finally:
        client.close()
    return {"status": "ok", "database": settings.db_name}


@app.post("/trigger-ingestion")
def trigger_ingestion_endpoint(background_tasks: BackgroundTasks) -> dict[str, str]:
    """
    Manually trigger the full ingestion pipeline.
    
    This endpoint allows on-demand execution of the ingestion process
    without waiting for the scheduled cron job. It runs independently
    and does not interfere with the scheduled daily runs.
    
    The ingestion job runs in the background and the endpoint returns immediately.
    Check the logs for job completion status and metrics.
    
    Returns:
        Dictionary confirming that the ingestion job has been queued.
    """
    logger.info("Received request to trigger manual ingestion")
    
    background_tasks.add_task(_run_ingestion_job)
    
    return {"status": "received"}


@app.get("/revalidate-school-entity")
def revalidate_school_entity_endpoint(background_tasks: BackgroundTasks) -> dict[str, str]:
    """Trigger revalidation of school entities into the configured S3 bucket."""

    settings = get_settings()
    logger.info("Received request to revalidate school entities")

    background_tasks.add_task(_run_revalidate_school_entity_job, settings)

    return {"status": "received"}


@app.get("/load-opendosm-polygons")
def load_opendosm_polygons_endpoint(background_tasks: BackgroundTasks) -> dict[str, str]:
    """Trigger loading of Negeri + Parlimen polygons from S3 to MongoDB."""

    def load_polygons_sequentially():
        try:
            negeri= load_opendosm_negeri.main()
            parlimen = load_opendosm_parlimen.main()
            logger.info(f"Negeri summary: {negeri}")
            logger.info(f"Parlimen summary: {parlimen}")
        except Exception as e:
            logger.exception("Error occurred while loading polygons: %s", e)

    background_tasks.add_task(load_polygons_sequentially)

    return {"status": "received request to load polygons"}
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
