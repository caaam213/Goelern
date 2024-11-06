import json
from typing import Any, Union


def load_json(data:dict)->Union[Any, None]:
    """Load JSON data

    Args:
        data (dict): Data to be loaded in JSON format

    Returns:
        Any : Loaded JSON data
    """
    if not data:
        return None
    
    try:
        return json.loads(data)
    except json.JSONDecodeError:
        print("Data is not in JSON format")
        return None