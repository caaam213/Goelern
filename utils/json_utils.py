import json


def load_json(data):
    try:
        return json.loads(data)
    except json.JSONDecodeError:
        print("Data is not in JSON format")
        return None