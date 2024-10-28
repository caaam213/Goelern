import os
from typing import Union

import pandas as pd


def save_data(file_path: str, data: str):
    """Save data to a file

    Args:
        path (str): Path to save the file
        data (str): Data to be saved
    """
    with open(file_path, "w") as file:
        file.write(data)


def verify_if_file_exists(file_path: str) -> bool:
    """Verify if file exists

    Args:
        file_path (str): File path to verify

    Returns:
        bool: True if file exists else false
    """
    return os.path.exists(file_path)


def load_data(file_path: str) -> Union[str, None]:
    """Load data using file path

    Args:
        file_path (str): File path to load data

    Returns:
        Union[str, None]: File content or None if the file doesn't exist
    """
    # Verify if the file exists
    if not verify_if_file_exists(file_path):
        return None

    # Open it if exists
    with open(file_path, "r") as file:
        return file.read()


def load_csv(file_path: str) -> Union[pd.DataFrame, None]:
    """Load csv file

    Args:
        file_path (str): File path to load data

    Returns:
        Union[pd.DataFrame, None]: dataframe content or None if the file doesn't exist
    """
    # Verify if the file exists
    if not verify_if_file_exists(file_path):
        return None

    # Open it if exists
    return pd.read_csv(file_path)
