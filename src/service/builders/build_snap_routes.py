from pymongo.errors import PyMongoError
from botocore.exceptions import ClientError

from src.config.settings import get_settings
from src.core.db import get_mongo_client
from src.core.s3 import upload_json_to_s3


FIXED_ROUTES = [
    "/",
    "/home",
    "/about",
    "/carian-sekolah",
    "/siaran",
]


def build_snap_routes(docs):
    routes = list(FIXED_ROUTES)
    for doc in docs:
        code = doc.get("KODSEKOLAH") or str(doc.get("_id"))
        routes.append(f"/halaman-sekolah/{code}")
    return routes


def generate_and_upload_snap_routes() -> int:
    """Generate snap-routes.json from MongoDB and upload to S3.

    Returns the number of routes generated.
    May raise PyMongoError or ClientError which the API layer will handle.
    """

    settings = get_settings()
    client = get_mongo_client()
    try:
        db = client[settings.db_name]
        collection = db[settings.entiti_sekolah_collection]
        docs = list(collection.find({}, {"_id": 1, "KODSEKOLAH": 1}))
    except PyMongoError:
        raise
    finally:
        client.close()

    payload = build_snap_routes(docs)

    try:
        upload_json_to_s3(payload, settings.s3_bucket_name, "common/snap-routes.json")
    except ClientError:
        raise

    return len(payload)