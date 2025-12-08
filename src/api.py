from __future__ import annotations

import logging
from typing import Any

from botocore.exceptions import ClientError
from fastapi import BackgroundTasks, FastAPI, HTTPException
from pymongo import MongoClient
from pymongo.errors import PyMongoError

from src.config.settings import get_settings
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



@app.get("/revalidate-school-entity")
def revalidate_school_entity_endpoint(background_tasks: BackgroundTasks) -> dict[str, str]:
    """Trigger revalidation of school entities into the configured S3 bucket."""

    settings = get_settings()
    logger.info("Received request to revalidate school entities")

    background_tasks.add_task(_run_revalidate_school_entity_job, settings)

    return {"status": "received"}
