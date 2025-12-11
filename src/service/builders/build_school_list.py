from pymongo.errors import PyMongoError
from botocore.exceptions import ClientError

from src.config.settings import get_settings
from src.core.db import get_mongo_client
from src.core.s3 import upload_json_to_s3


def build_school_list(docs):
    mapping = []

    for doc in docs:
        key = doc.get("kodSekolah")
        value = doc.get("namaSekolah")

        if key and value:
            mapping.append({"KODSEKOLAH": key, "NAMASEKOLAH": value})

    return mapping


def generate_and_upload_school_list() -> int:
    """Generate school-list.json from MongoDB and upload to S3.

    Returns the number of school entries generated.
    May raise PyMongoError or ClientError which the API layer will handle.
    """

    settings = get_settings()

    # Read from MongoDB
    client = get_mongo_client()
    try:
        db = client[settings.db_name]
        collection = db[settings.entiti_sekolah_collection]
        docs = list(
            collection.find({}, {"_id": 1, "kodSekolah": 1, "namaSekolah": 1})
        )
    except PyMongoError:
        raise
    finally:
        client.close()

    payload = build_school_list(docs)

    try:
        upload_json_to_s3(payload, settings.s3_bucket_name, "common/school-list.json")
    except ClientError:
        raise

    return len(payload)