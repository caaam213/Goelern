import os
from typing import Any, Union

from utils.utils_mongo.connection import create_client
import logging
from typing import Optional, Dict, Any
from airflow.providers.mongo.hooks.mongo import MongoHook

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


def create_connection(conn_id: str)->Union[MongoHook, None]:
    """Create a connection to MongoDB

    Args:
        conn_id (str): Connection id to connect to MongoDB

    Returns:
        Union[MongoHook, None]: Return a connection to MongoDB
    """
    try:
        mongo_hook = MongoHook(conn_id=conn_id)
        return mongo_hook
    except Exception as e:
        logging.error(f"Error connecting to MongoDB: {e}")
        return None
    

def find_data( 
    mongo_hook: MongoHook,
    db_name: str, 
    collection_name: str, 
    query_filter: Dict[str, Any],
    only_one: bool = False
) -> Dict:
    """
    Retrieves data from a specified MongoDB collection based on given search criteria.

    Args:
        mongo_hook (MongoHook): Airflow hook for MongoDB connection.
        db_name (str): Name of the MongoDB database.
        collection_name (str): Name of the MongoDB collection.
        query_filter (Dict[str, Any]): Dictionary of search criteria.
        only_one (bool): If True, returns only the first document found else returns all documents found. Defaults to False.

    Returns:
        Dict: Document found matching the criteria 
    """
    try:
        db = mongo_hook.get_conn()[db_name]
        collection = db[collection_name]

        # Search based on specified criteria
        if only_one:
            result = collection.find_one(query_filter)
        else:
            result = collection.find(query_filter)
        if result:
            logging.info(f"Document found in '{collection_name}' matching criteria {query_filter}.")
        else:
            logging.warning(f"No document found in '{collection_name}' matching criteria {query_filter}.")

        return result
    except Exception as e:
        logging.error(f"Error retrieving data from '{collection_name}': {e}")
        return {}

