import os
import logging
from typing import Optional

from fastapi import APIRouter, Header, HTTPException, status
from pymongo.errors import PyMongoError
from botocore.exceptions import ClientError

from src.core.db import get_entitisekolah_collection
from src.core.jsonhelpers import build_snap_routes, build_school_list
from src.core.s3 import upload_json_to_s3

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/dataproc", tags=["dataproc"])

S3_PREFIX = "common"
SNAP_ROUTES_KEY = f"{S3_PREFIX}/snap-routes.json"
SCHOOL_LIST_KEY = f"{S3_PREFIX}/school-list.json"

S3_BUCKET = os.getenv("S3_BUCKET_DATAPROC") 


def _check_api_key(x_api_key: Optional[str]) -> bool:
    expected = os.getenv("DATAPROC_API_KEY")
    return x_api_key == expected


@router.post("/generate/snap-routes")
def generate_snap_routes(x_api_key: Optional[str] = Header(None)):
    if not _check_api_key(x_api_key):
        raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="Invalid API key")

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
def generate_school_list(x_api_key: Optional[str] = Header(None)):
    if not _check_api_key(x_api_key):
        raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="Invalid API key")

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
