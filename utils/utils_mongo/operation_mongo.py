import os
from typing import Any

from utils.utils_mongo.connection import create_client


def add_many_data(collection_name: str, data: Any):
    """Add many data in mongo

    Args:
        collection_name (str): Collection to add data
        data (Any): data to add
    """
    client = create_client()

    db = client[os.environ["MONGO_DB_DEV"]]

    collection = db[collection_name]

    print(data)

    collection.insert_many(data)
