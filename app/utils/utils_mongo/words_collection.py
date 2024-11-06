from typing import Any
from utils.utils_mongo.operation_mongo import add_many_data


def add_many_words(data: Any):
    """Call add_many_data to add several data in words collection

    Args:
        data (Any): Data to add
    """
    add_many_data("words", data)
