from datetime import datetime
from io import StringIO
import json
import logging
import os
import sys
from typing import Any


import numpy as np
import pandas as pd
from pyspark.sql import SparkSession
from pyspark.sql import DataFrame
from sklearn.cluster import KMeans
from sklearn.metrics import silhouette_score
from sklearn.model_selection import GridSearchCV
from sklearn.pipeline import Pipeline
from sklearn.preprocessing import StandardScaler
from airflow.providers.mongo.hooks.mongo import MongoHook


sys.path.insert(0, "/opt/airflow/")
from constants.difficulty_constants import LEVEL1, LEVEL2, LEVEL3
from constants.collections_constants import (
    COLLECTION_CONSTANTS,
    COLLECTION_PARAMETERS,
    COLLECTION_WORDS,
    FIELD_CREATED_DATE,
    FIELD_DIFFICULTY,
    FIELD_FILE_NAME,
    FIELD_LANGUAGE,
    FIELD_NUMBER_CHARS,
    FIELD_SCRAP_URL,
    FIELD_SIMILARITY_SCORE,
    FIELD_STATUS,
)
from constants.error_constants import (
    LANG_DICT_NOT_FOUND_ERROR,
    MONGO_DATA_NOT_FOUND,
    S3_DATA_NOT_FOUND,
)
from constants.status_constants import STATUS_FINISHED, STATUS_WAITING_AI_PROCESSING
from etl.ml.manage_data import get_train_data

sys.path.insert(0, "/opt/")
from utils.utils_ml.load_save_model import load_trained_model, save_trained_model
from utils.utils_mongo.operation_mongo import add_data, find_data, update_data
from utils.utils_files.s3_utils import get_data_from_s3, save_data_on_s3
from constants.s3_constants import POSTPROCESSED_DATA, PREPROCESSED_DATA, S3_BUCKET
from constants.file_constants import TRAINED_MODEL_PATH_FILE


class PredictDifficulty:

    def _clean_data(self, df_spark: DataFrame) -> np.array:
        """Clean and prepare data for ML pipeline

        Args:
            df_spark (pyspark.DataFrame): Data in pyspark DataFrame format to clean

        Returns:
            np.array: Data in numpy array format for ML pipeline
        """
        # Drop nulls and duplicates
        df_spark = df_spark.dropna().drop_duplicates()

        # Drop irrelevant columns
        df_spark = (
            df_spark.select(FIELD_SIMILARITY_SCORE, FIELD_NUMBER_CHARS)
            .rdd.map(lambda row: (row[0], row[1]))
            .collect()
        )

        data_array = np.array(df_spark)

        return data_array

    def _create_ml(self, data_lang: dict) -> Any:
        """Create a machine learning model to predict difficulty

        Args:
            data_lang (dict): Language data to use for training

        Raises:
            Exception: Problem with generating training data

        Returns:
            Any: Best estimator from the grid search
        """

        lang = data_lang.get(FIELD_LANGUAGE)
        # Load training data
        data = get_train_data(lang)

        if data.empty:
            raise Exception("Problem with generating training data")

        spark = SparkSession.builder.appName("Vocabulary").getOrCreate()
        df = spark.createDataFrame(data)

        # Clean and prepare data for ML pipeline
        data_array = self._clean_data(df)

        pipeline = Pipeline(
            [("scaler", StandardScaler()), ("clusterizer", KMeans(n_clusters=3))]
        )

        # Set up grid search parameters
        param_grid = {
            "clusterizer__n_clusters": [3],
            "clusterizer__max_iter": [300, 500, 1000],
        }

        grid_search = GridSearchCV(pipeline, param_grid, cv=5)
        grid_search.fit(data_array)

        # Save the model
        save_trained_model(TRAINED_MODEL_PATH_FILE.format(lang), grid_search)

        return grid_search.best_estimator_

    def _predict(self, model: Any, X: pd.DataFrame) -> np.array:
        """Predict the difficulty of the words

        Args:
            model (Any): Model to use for prediction
            X (pd.DataFrame): Data to predict

        Returns:
            np.array: Predictions of the difficulty of the words
        """
        spark = SparkSession.builder.appName("Vocabulary").getOrCreate()
        df = spark.createDataFrame(X)

        data_array = self._clean_data(df)

        # evaluate model
        y_predict = model.predict(data_array)

        # check results
        logging.info(silhouette_score(data_array, y_predict))

        # Return the predictions
        return y_predict

    def _get_and_attribute_difficulty(
        self,
        data_predict: pd.DataFrame,
        data_lang: dict,
    ) -> pd.DataFrame:
        """Make a prediction and attribute the difficulty of each word predicted by the model

        Args:
            data_predict (pd.DataFrame): Data to predict the difficulty of the words
            data_lang (dict): Language data

        Returns:
            pd.DataFrame: Data with the difficulty of the words predicted by the model
        """
        lang = data_lang.get(FIELD_LANGUAGE)

        # Get model
        model = load_trained_model(TRAINED_MODEL_PATH_FILE.format(lang))

        if not model:
            model = self._create_ml(data_lang)
        else:
            model = model.best_estimator_

        predictions = self._predict(model, data_predict.copy())

        if hasattr(model, "best_estimator_"):
            kmeans_model = model.best_estimator_.named_steps["clusterizer"]
        else:
            kmeans_model = model.named_steps["clusterizer"]

        centers = kmeans_model.cluster_centers_

        center_sums = np.sum(centers, axis=1)
        sorted_indices = np.argsort(center_sums)

        # Mapping the predictions to the difficulty labels
        difficulty_labels = {
            sorted_indices[2]: LEVEL1,
            sorted_indices[1]: LEVEL2,
            sorted_indices[0]: LEVEL3,
        }

        mapped_difficulties = [difficulty_labels[p] for p in predictions]

        data_predict[FIELD_DIFFICULTY] = mapped_difficulties

        return data_predict

    def run(self, mongo_hook: MongoHook):
        """Run the ML pipeline to predict the difficulty of the words

        Args:
            mongo_hook (MongoHook): MongoHook to connect to MongoDB

        """

        date = datetime.now().strftime("%Y-%m-%d")
        file_info = find_data(
            mongo_hook,
            os.environ["MONGO_DB_DEV"],
            COLLECTION_PARAMETERS,
            {FIELD_STATUS: STATUS_WAITING_AI_PROCESSING, FIELD_CREATED_DATE: date},
            True,
        )

        if not file_info:
            raise ValueError(MONGO_DATA_NOT_FOUND)

        # Get info
        file_name = file_info.get(FIELD_FILE_NAME)
        pre_file_name = f"{PREPROCESSED_DATA}/{file_name}"
        ext_file_name = f"{POSTPROCESSED_DATA}/{file_name}"
        scrap_url = file_info.get(FIELD_SCRAP_URL)
        lang = file_info.get(FIELD_LANGUAGE)

        data_lang = find_data(
            mongo_hook,
            os.environ["MONGO_DB_DEV"],
            COLLECTION_CONSTANTS,
            {FIELD_LANGUAGE: lang},
            True,
        )
        logging.info(data_lang)

        # If data_lang is empty, return an empty list
        if not data_lang:
            raise Exception(LANG_DICT_NOT_FOUND_ERROR.format(lang))

        file_content = get_data_from_s3(S3_BUCKET, pre_file_name)

        if not file_content:
            raise Exception(S3_DATA_NOT_FOUND)

        file_content = file_content.decode("utf-8")
        csv_data = StringIO(file_content)

        # Load into pandas DataFrame
        data_predict = pd.read_csv(csv_data)

        # Get the difficulty of the words predicted by the model
        data_predict = self._get_and_attribute_difficulty(data_predict, data_lang)

        # Save the predictions to s3
        save_data_on_s3(
            S3_BUCKET,
            ext_file_name,
            data_predict.to_csv(index=False),
        )

        # Add the file name to mongodb
        update_data(
            mongo_hook,
            os.environ["MONGO_DB_DEV"],
            COLLECTION_PARAMETERS,
            {
                FIELD_SCRAP_URL: scrap_url,
                FIELD_LANGUAGE: lang,
                FIELD_FILE_NAME: file_name,
            },
            {
                FIELD_FILE_NAME: file_name,
                FIELD_STATUS: STATUS_FINISHED,
                FIELD_LANGUAGE: lang,
                FIELD_CREATED_DATE: date,
                FIELD_SCRAP_URL: scrap_url,
            },
        )

        # Store the data in MongoDB
        data_json = data_predict.to_json(orient="records")
        data_json = json.loads(data_json)
        add_data(mongo_hook, os.environ["MONGO_DB_DEV"], COLLECTION_WORDS, data_json)
