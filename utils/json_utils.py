import json


def load_json(data):
    if not data:
        return None
    
    try:
        return json.loads(data)
    except json.JSONDecodeError:
        print("Data is not in JSON format")
        return None