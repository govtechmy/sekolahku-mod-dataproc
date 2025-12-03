from __future__ import annotations

import json
import logging
from concurrent.futures import ThreadPoolExecutor
from typing import Any

from botocore.exceptions import ClientError
from pymongo import MongoClient
from pymongo.collection import Collection
from pymongo.errors import PyMongoError

from src.config.settings import Settings
from src.core.aws import get_s3_client, get_s3_bucket_name

logger = logging.getLogger(__name__)

BATCH_SIZE = 100
MAX_WORKERS = 10
TEMP_PREFIX = "temp"


def _normalise_segment(value: Any, fallback: str) -> str:
    text = (str(value).strip() if value else fallback).strip()
    if not text:
        text = fallback
    return text.replace("/", "-").replace(" ", "_")


def _dumps_document(document: dict[str, Any]) -> bytes:
    return json.dumps(document, default=str, ensure_ascii=False).encode("utf-8")


def _build_parlimen_path(document: dict[str, Any]) -> tuple[str, str, str]:
    data = document.get("data") or {}
    pentadbiran = data.get("infoPentadbiran") or {}

    negeri = _normalise_segment(pentadbiran.get("negeri"), "UNKNOWN_NEGERI")
    parlimen = _normalise_segment(pentadbiran.get("parlimen"), "UNKNOWN_PARLIMEN")
    kod_sekolah = _normalise_segment(document.get("kodSekolah"), "UNKNOWN_KOD")

    return negeri, parlimen, kod_sekolah


def _upload_to_s3(s3_client, bucket: str, temp_key: str, payload: bytes, kod_sekolah: str):
    try:
        s3_client.put_object(
            Bucket=bucket,
            Key=temp_key,
            Body=payload,
            ContentType="application/json",
        )
    except ClientError:
        logger.exception("Failed to upload sekolah=%s to temp key=%s", kod_sekolah, temp_key)


def revalidate_school_entity(settings: Settings) -> dict[str, Any]:
    mongo_client = MongoClient(settings.mongo_uri, serverSelectionTimeoutMS=5000)
    s3_client = get_s3_client()
    bucket = get_s3_bucket_name()
    uploaded_temp_keys: list[str] = []
    processed = 0

    try:
        collection: Collection = mongo_client[settings.db_name][settings.entiti_sekolah_collection]
        logger.info(
            "Starting EntitiSekolah revalidation: bucket=%s collection=%s.%s",
            bucket,
            settings.db_name,
            settings.entiti_sekolah_collection,
        )
        logger.debug(
            "Fetching documents from MongoDB: db=%s collection=%s",
            settings.db_name,
            settings.entiti_sekolah_collection,
        )
        cursor = collection.find({}, {"_id": 0}).batch_size(BATCH_SIZE)

        with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
            futures = []
            for document in cursor:
                negeri, parlimen, kod_sekolah = _build_parlimen_path(document)
                temp_key = f"{TEMP_PREFIX}/{negeri}/{parlimen}/{kod_sekolah}.json"

                payload = _dumps_document(document)
                logger.debug("Uploading sekolah=%s to temp key=%s", kod_sekolah, temp_key)
                future = executor.submit(_upload_to_s3, s3_client, bucket, temp_key, payload, kod_sekolah)
                futures.append(future)

                uploaded_temp_keys.append(temp_key)
                processed += 1

            # Wait for all uploads to complete
            for future in futures:
                future.result()

        final_keys: list[str] = []
        for temp_key in uploaded_temp_keys:
            final_key = temp_key[len(TEMP_PREFIX) + 1 :]
            copy_source = {"Bucket": bucket, "Key": temp_key}

            logger.debug("Copying temp key=%s to final key=%s", temp_key, final_key)
            try:
                s3_client.copy_object(Bucket=bucket, CopySource=copy_source, Key=final_key)
            except ClientError:
                logger.exception("Failed to copy object from %s to %s", temp_key, final_key)
                raise

            logger.debug("Deleting temp key=%s after successful copy", temp_key)
            try:
                s3_client.delete_object(Bucket=bucket, Key=temp_key)
            except ClientError:
                logger.exception("Failed to delete temp object %s", temp_key)
                raise

            final_keys.append(final_key)

        logger.info(
            "EntitiSekolah revalidation complete: processed=%d bucket=%s finalized=%d",
            processed,
            bucket,
            len(final_keys),
        )

        return {
            "bucket": bucket,
            "processed": processed,
            "finalized_keys": final_keys,
        }
    except PyMongoError:
        logger.exception("MongoDB error occurred during EntitiSekolah revalidation")
        raise
    except ClientError:
        logger.exception("S3 error occurred during EntitiSekolah revalidation")
        raise
    finally:
        mongo_client.close()