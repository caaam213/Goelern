import os


def save_data(path, data):
    with open(path, "w") as file:
        file.write(data)

def load_data(path):
    # Verify if the file exists
    if not os.path.exists(path):
        return None
    
    with open(path, "r") as file:
        return file.read()