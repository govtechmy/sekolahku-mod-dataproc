from pymongo import MongoClient
import os

def get_mongo_client():
    uri = os.getenv("MONGO_URI")
    if not uri:
        raise ValueError("MONGO_URI environment variable is not set")
    return MongoClient(uri)

def get_entitisekolah_collection():
    client = get_mongo_client()
    db_name = os.getenv("DB_NAME", "sekolahku")
    db = client[db_name]
    return db["EntitiSekolah"]