import sys
from typing import Union
import pandas as pd

sys.path.insert(0, "/opt/airflow/")
from constants.file_constants import VOCABULARY_FILE, VOCABULARY_FILE_2
from utils.utils_files.local_files_utils import load_csv


def get_train_data(lang: str) -> Union[pd.DataFrame, None]:
    """
    Open an existing data if the file exists
    Args:
        lang (str): Targetted language

    Returns:
        pd.DataFrame: Data to train a model in pandas dataframe format or None if lang is not valid
    """

    # Verify if the file doesnt exist
    data = load_csv(VOCABULARY_FILE.format(lang))
    
    # Save the data in a csv file
    data.to_csv(VOCABULARY_FILE_2.format(lang), index=False)
    
    return data
