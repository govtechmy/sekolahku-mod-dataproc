from __future__ import annotations

import logging
from typing import Any

from botocore.exceptions import ClientError
from fastapi import FastAPI, HTTPException
from pymongo import MongoClient
from pymongo.errors import PyMongoError

from src.config.settings import get_settings
from src.service.entitiSekolah import revalidate_school_entity

if not logging.getLogger().handlers:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
    )

logger = logging.getLogger(__name__)
app = FastAPI()


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
