from typing import Any, Union, Dict, Any
import logging
from airflow.providers.mongo.hooks.mongo import MongoHook

def remove_data(mongo_hook: MongoHook, db_name: str, collection_name: str, query_filter: Dict[str, Any])->bool:
    """Remove data from a collection in MongoDB

    Args:
        mongo_hook (MongoHook): Airflow hook for MongoDB connection.
        db_name (str): Name of the MongoDB database.
        collection_name (str): Name of the MongoDB collection.
        query_filter (Dict[str, Any]): Dictionary of search criteria.

    Returns:
        bool: True if the data is removed successfully, False otherwise
    """
    try:
        db = mongo_hook.get_conn()[db_name]
        collection = db[collection_name]

        result = collection.delete_many(query_filter)
        if result:
            logging.info(f"Document(s) removed from '{collection_name}'.")
        else:
            logging.warning(f"No document removed from '{collection_name}'.")

        return True
    except Exception as e:
        logging.error(f"Error when removing data from '{collection_name}': {e}")
        return False

def add_data(mongo_hook: MongoHook,
    db_name: str, 
    collection_name: str, data: Any, insert_one: bool = False)->bool:
    """Add many data in mongo

    Args:
        mongo_hook (MongoHook): Airflow hook for MongoDB connection.
        db_name (str): Name of the MongoDB database.
        collection_name (str): Name of the MongoDB collection.
        data (Any): Data to add in the collection
    
    Returns:
        bool: True if the data is added successfully, False otherwise
    """
    try:
        db = mongo_hook.get_conn()[db_name]
        collection = db[collection_name]

        if insert_one:
            result = collection.insert_one(data)
        else:
            result = collection.insert_many(data)
            
        if result:
            logging.info(f"Document(s) added to '{collection_name}'.")
        else:
            logging.warning(f"No document added to '{collection_name}'.")

        return True
    except Exception as e:
        logging.error(f"Error when adding data from '{collection_name}': {e}")
        return False

def update_data(mongo_hook: MongoHook, db_name: str, collection_name: str, query_filter: Dict[str, Any], new_values: Dict[str, Any], update_one: bool = False)->bool:
    """Update data in a collection in MongoDB

    Args:
        mongo_hook (MongoHook): Airflow hook for MongoDB connection.
        db_name (str): Name of the MongoDB database.
        collection_name (str): Name of the MongoDB collection.
        query_filter (Dict[str, Any]): Dictionary of search criteria.
        new_values (Dict[str, Any]): Dictionary of new values to update.
        update_one (bool): If True, updates only the first document found else updates all documents found. Defaults to False.

    Returns:
        bool: True if the data is updated successfully, False otherwise
    """
    try:
        db = mongo_hook.get_conn()[db_name]
        collection = db[collection_name]

        if update_one:
            result = collection.update_one(query_filter, {"$set": new_values})
        else:
            result = collection.update_many(query_filter, {"$set": new_values})
            
        if result:
            logging.info(f"Document(s) updated in '{collection_name}'.")
        else:
            logging.warning(f"No document updated in '{collection_name}'.")

        return True
    except Exception as e:
        logging.error(f"Error when updating data from '{collection_name}': {e}")
        return False    


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
    only_one: bool = False,
    projection: Dict[str, Any] = {}
) -> Dict:
    """
    Retrieves data from a specified MongoDB collection based on given search criteria.

    Args:
        mongo_hook (MongoHook): Airflow hook for MongoDB connection.
        db_name (str): Name of the MongoDB database.
        collection_name (str): Name of the MongoDB collection.
        query_filter (Dict[str, Any]): Dictionary of search criteria.
        only_one (bool): If True, returns only the first document found else returns all documents found. Defaults to False.
        projection (Dict[str, Any]): Dictionary of fields to include or exclude in the result. Defaults to {}.
    Returns:
        Dict: Document found matching the criteria 
    """
    try:
        db = mongo_hook.get_conn()[db_name]
        collection = db[collection_name]

        # Search based on specified criteria
        if only_one:
            result = collection.find_one(query_filter, projection)
        else:
            result = collection.find(query_filter, projection)
        if result:
            logging.info(f"Document found in '{collection_name}' matching criteria {query_filter}.")
        else:
            logging.warning(f"No document found in '{collection_name}' matching criteria {query_filter}.")

        return result
    except Exception as e:
        logging.error(f"Error retrieving data from '{collection_name}': {e}")
        return {}

