from __future__ import annotations

import logging
import time
from datetime import datetime
from typing import Any

from botocore.exceptions import ClientError
from fastapi import BackgroundTasks, FastAPI, HTTPException
from fastapi import FastAPI, HTTPException
from fastapi_crons import Crons
from pymongo import MongoClient
from pymongo.errors import PyMongoError

from src.config.settings import get_settings
from src.main import run_ingest
from src.service.entitiRevalidate import revalidate_school_entity

if not logging.getLogger().handlers:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
    )

logger = logging.getLogger(__name__)
app = FastAPI()


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
def trigger_ingestion_endpoint() -> dict[str, Any]:
    """
    Manually trigger the full ingestion pipeline.
    
    This endpoint allows on-demand execution of the ingestion process
    without waiting for the scheduled cron job. It runs independently
    and does not interfere with the scheduled daily runs.
    
    Returns:
        Dictionary containing the ingestion summary with metrics from all pipeline stages.
    """
    start_time = time.time()
    start_timestamp = datetime.now().isoformat()
    
    logger.info("Manual ingestion triggered via POST /trigger-ingestion")
    
    try:
        summary = run_ingest()
        
        elapsed_time = time.time() - start_time
        end_timestamp = datetime.now().isoformat()
        
        logger.info("Manual ingestion completed successfully in %.2f seconds", elapsed_time)
        
        return {
            "status": "ok",
            "trigger": "manual",
            "start_time": start_timestamp,
            "end_time": end_timestamp,
            "duration_seconds": round(elapsed_time, 2),
            **summary
        }
        
    except PyMongoError as exc:
        elapsed_time = time.time() - start_time
        logger.error("Manual ingestion failed - database error (%.2f seconds): %s", elapsed_time, str(exc))
        logger.exception("Database error during manual ingestion:")
        raise HTTPException(status_code=503, detail="Database unreachable") from exc
    except Exception as exc:
        elapsed_time = time.time() - start_time
        logger.error("Manual ingestion failed - unexpected error (%.2f seconds): %s", elapsed_time, str(exc))
        logger.exception("Error during manual ingestion:")
        raise HTTPException(status_code=500, detail=f"Ingestion failed: {str(exc)}") from exc


@app.get("/revalidate-school-entity")
def revalidate_school_entity_endpoint(background_tasks: BackgroundTasks) -> dict[str, str]:
    """Trigger revalidation of school entities into the configured S3 bucket."""

    settings = get_settings()
    logger.info("Received request to revalidate school entities")

    background_tasks.add_task(_run_revalidate_school_entity_job, settings)

    return {"status": "received"}
    try:
        summary = revalidate_school_entity(settings)
    except PyMongoError as exc:
        logger.exception("MongoDB error while handling revalidation request")
        raise HTTPException(status_code=503, detail="Database unreachable") from exc
    except ClientError as exc:
        logger.exception("S3 error while handling revalidation request")
        raise HTTPException(status_code=502, detail="S3 operation failed") from exc

    logger.info(
        "Revalidation completed successfully: bucket=%s processed=%s",
        summary.get("bucket"),
        summary.get("processed"),
    )

    return {"status": "ok", **summary}


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
