import os
import sys
from typing import Union



from constants.mongo_constants import CONSTANTS_TABLE
from utils.utils_mongo.connection import create_client


def get_data_by_lang(lang: str) -> Union[dict, None]:
    """Get data by language

    Args:
        lang (str): Language to get the data

    Returns:
        Union[dict, None]: Data from the language or None if the language does not exist in the collection
    """
    client = create_client()

    db = client[os.environ["MONGO_DB_DEV"]]

    collection = db[CONSTANTS_TABLE]

    lang_data = collection.find_one({"language": lang})

    return lang_data
