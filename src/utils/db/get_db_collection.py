from pymongo import MongoClient
from src.config.settings import Settings
from pymongo.collection import Collection

def get_db_collection(settings: Settings, name: str) -> Collection:
    """
    Get a MongoDB collection instance based on the provided settings and collection name.
    """
    client = MongoClient(settings.mongo_uri)
    return client[settings.db_name][name]