from __future__ import annotations

import logging
import time
from datetime import datetime
from typing import Any

from botocore.exceptions import ClientError
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
    
    Note: Runs in server's local timezone. For production, ensure server timezone is set correctly.
    """
    start_time = time.time()
    start_timestamp = datetime.now().isoformat()
    
    logger.info("=" * 80)
    logger.info("SCHEDULED INGESTION JOB STARTED")
    logger.info("Start Time: %s", start_timestamp)
    logger.info("=" * 80)
    
    try:
        summary = run_ingest()
        
        elapsed_time = time.time() - start_time
        end_timestamp = datetime.now().isoformat()
        
        # Extract key metrics from summary
        ingestion_result = summary.get("ingestion", {})
        entiti_result = summary.get("entiti", {})
        negeri_result = summary.get("negeri_parlimen_kod_sekolah", {})
        analitik_result = summary.get("analitik", {})
        
        logger.info("=" * 80)
        logger.info("SCHEDULED INGESTION JOB COMPLETED SUCCESSFULLY")
        logger.info("End Time: %s", end_timestamp)
        logger.info("Duration: %.2f seconds", elapsed_time)
        logger.info("-" * 80)
        logger.info("INGESTION METRICS:")
        logger.info("  - Total Processed: %s", ingestion_result.get("processed", 0))
        logger.info("  - Inserted: %s", ingestion_result.get("inserted", 0))
        logger.info("  - Updated: %s", ingestion_result.get("updated", 0))
        logger.info("  - Inactivated: %s", ingestion_result.get("inactivated", 0))
        logger.info("  - Failed: %s", ingestion_result.get("failed", 0))
        logger.info("  - Entiti Synced: %s", ingestion_result.get("entiti_synced", 0))
        logger.info("-" * 80)
        logger.info("ENTITI SEKOLAH METRICS:")
        logger.info("  - Processed: %s", entiti_result.get("processed", 0))
        logger.info("  - Inserted: %s", entiti_result.get("inserted", 0))
        logger.info("  - Updated: %s", entiti_result.get("updated", 0))
        logger.info("-" * 80)
        logger.info("NEGERI PARLIMEN KOD SEKOLAH METRICS:")
        logger.info("  - Processed: %s", negeri_result.get("processed", 0))
        logger.info("  - Inserted: %s", negeri_result.get("inserted", 0))
        logger.info("  - Updated: %s", negeri_result.get("updated", 0))
        if analitik_result:
            logger.info("-" * 80)
            logger.info("ANALITIK SEKOLAH METRICS:")
            logger.info("  - Processed: %s", analitik_result.get("processed", 0))
            logger.info("  - Inserted: %s", analitik_result.get("inserted", 0))
            logger.info("  - Updated: %s", analitik_result.get("updated", 0))
        else:
            logger.info("-" * 80)
            logger.info("ANALITIK SEKOLAH: Skipped (no data changes)")
        logger.info("=" * 80)
        
    except PyMongoError as exc:
        elapsed_time = time.time() - start_time
        end_timestamp = datetime.now().isoformat()
        logger.error("=" * 80)
        logger.error("SCHEDULED INGESTION JOB FAILED - DATABASE ERROR")
        logger.error("End Time: %s", end_timestamp)
        logger.error("Duration: %.2f seconds", elapsed_time)
        logger.error("Error Type: %s", type(exc).__name__)
        logger.error("Error Message: %s", str(exc))
        logger.error("=" * 80)
        logger.exception("Full database error details:")
        logger.error("Job failed but FastAPI server continues running")
        logger.error("Next scheduled run: tomorrow at 00:00")
        # DO NOT re-raise - allow the server to continue running
    except Exception as exc:
        elapsed_time = time.time() - start_time
        end_timestamp = datetime.now().isoformat()
        logger.error("=" * 80)
        logger.error("SCHEDULED INGESTION JOB FAILED - UNEXPECTED ERROR")
        logger.error("End Time: %s", end_timestamp)
        logger.error("Duration: %.2f seconds", elapsed_time)
        logger.error("Error Type: %s", type(exc).__name__)
        logger.error("Error Message: %s", str(exc))
        logger.error("=" * 80)
        logger.exception("Full error details:")
        logger.error("Job failed but FastAPI server continues running")
        logger.error("Next scheduled run: tomorrow at 00:00")
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
    
    logger.info("=" * 80)
    logger.info("MANUAL INGESTION JOB TRIGGERED")
    logger.info("Start Time: %s", start_timestamp)
    logger.info("Triggered via: POST /trigger-ingestion")
    logger.info("=" * 80)
    
    try:
        summary = run_ingest()
        
        elapsed_time = time.time() - start_time
        end_timestamp = datetime.now().isoformat()
        
        # Extract key metrics from summary
        ingestion_result = summary.get("ingestion", {})
        
        logger.info("=" * 80)
        logger.info("MANUAL INGESTION JOB COMPLETED SUCCESSFULLY")
        logger.info("End Time: %s", end_timestamp)
        logger.info("Duration: %.2f seconds", elapsed_time)
        logger.info("-" * 80)
        logger.info("INGESTION METRICS:")
        logger.info("  - Total Processed: %s", ingestion_result.get("processed", 0))
        logger.info("  - Inserted: %s", ingestion_result.get("inserted", 0))
        logger.info("  - Updated: %s", ingestion_result.get("updated", 0))
        logger.info("  - Inactivated: %s", ingestion_result.get("inactivated", 0))
        logger.info("  - Failed: %s", ingestion_result.get("failed", 0))
        logger.info("=" * 80)
        
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
        logger.error("=" * 80)
        logger.error("MANUAL INGESTION JOB FAILED - DATABASE ERROR")
        logger.error("Duration: %.2f seconds", elapsed_time)
        logger.error("Error Type: %s", type(exc).__name__)
        logger.error("=" * 80)
        logger.exception("Database error during manual ingestion:")
        raise HTTPException(status_code=503, detail="Database unreachable") from exc
    except Exception as exc:
        elapsed_time = time.time() - start_time
        logger.error("=" * 80)
        logger.error("MANUAL INGESTION JOB FAILED - UNEXPECTED ERROR")
        logger.error("Duration: %.2f seconds", elapsed_time)
        logger.error("Error Type: %s", type(exc).__name__)
        logger.error("=" * 80)
        logger.exception("Error during manual ingestion:")
        raise HTTPException(status_code=500, detail=f"Ingestion failed: {str(exc)}") from exc


@app.get("/revalidate-school-entity")
def revalidate_school_entity_endpoint() -> dict[str, Any]:
    """Trigger revalidation of school entities into the configured S3 bucket."""

    settings = get_settings()
    logger.info("Received request to revalidate school entities")

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
