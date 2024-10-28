import os
from pymongo import MongoClient


def create_client() -> MongoClient:
    """Create a MongoDB client

    Returns:
        MongoClient: MongoDB client
    """
    mongo_host = os.getenv("MONGO_HOST_DEV")
    mongo_port = os.getenv("MONGO_PORT_DEV")

    return MongoClient(f"mongodb://{mongo_host}:{mongo_port}/")
