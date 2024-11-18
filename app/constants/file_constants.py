"""
    Constants for the file names
"""

VOCABULARY_FILE = "/opt/airflow/data/data.csv"
SCRAP_URL_FILE_NAME = "urls_to_scrap_{}.json"
SCRAP_URLS_PATH_FILE = f"/opt/airflow/data/{SCRAP_URL_FILE_NAME}"
PREDICTION_PATH_FILE = "./ml/res/predictions_{}.csv"
TRAINED_MODEL_PATH_FILE = "/opt/airflow/etl/ml/artifacts/difficulty_model_{}.pkl"
