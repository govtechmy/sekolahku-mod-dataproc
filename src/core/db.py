from pymongo import MongoClient
import os

def get_mongo_client():
    uri = os.getenv("MONGO_URI")
    if not uri:
        raise ValueError("MONGO_URI environment variable is not set")
    return MongoClient(uri)