from __future__ import annotations

import logging
from concurrent.futures import ThreadPoolExecutor
from typing import Any

from botocore.exceptions import ClientError
from pymongo import MongoClient
from pymongo.collection import Collection
from pymongo.errors import PyMongoError

from src.config.settings import Settings
from src.core.aws import get_s3_client, get_s3_bucket_name
from .helpers import (
    build_parlimen_path,
    dumps_document,
    move_staged_objects,
    upload_to_s3,
)

logger = logging.getLogger(__name__)


def revalidate_school_entity(settings: Settings) -> dict[str, Any]:
    mongo_client = MongoClient(settings.mongo_uri, serverSelectionTimeoutMS=5000)
    s3_client = get_s3_client()
    bucket = get_s3_bucket_name()
    batch_size = settings.entiti_revalidate_batch_size
    max_workers = settings.entiti_revalidate_max_workers
    temp_prefix = settings.entiti_revalidate_temp_prefix
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
        logger.info(
            "Fetching documents from MongoDB: db=%s collection=%s",
            settings.db_name,
            settings.entiti_sekolah_collection,
        )
        cursor = collection.find({}, {"_id": 0}).batch_size(batch_size)

        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            futures = []
            logger.info("Uploading sekolah entities to temp folder")
            for document in cursor:
                negeri, parlimen, kod_sekolah = build_parlimen_path(document)
                temp_key = f"{temp_prefix}/{negeri}/{parlimen}/{kod_sekolah}/{kod_sekolah}.json"

                payload = dumps_document(document)
                logger.debug("Uploading sekolah=%s to temp key=%s", kod_sekolah, temp_key)
                future = executor.submit(upload_to_s3, s3_client, bucket, temp_key, payload, kod_sekolah)
                futures.append(future)

                uploaded_temp_keys.append(temp_key)
                processed += 1

            for future in futures:
                future.result()

        logger.info("Moving %d staged objects into final structure", len(uploaded_temp_keys))
        final_keys = move_staged_objects(
            s3_client,
            bucket,
            uploaded_temp_keys,
            temp_prefix=temp_prefix,
            max_workers=max_workers,
        )

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

