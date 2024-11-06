from typing import Any
import numpy as np
import pandas as pd
from pyspark.sql import SparkSession
from sklearn.cluster import KMeans
from sklearn.metrics import silhouette_score
from sklearn.model_selection import GridSearchCV
from sklearn.pipeline import Pipeline
from sklearn.preprocessing import StandardScaler

from app.constants.file_constants import PREDICTION_PATH_FILE, TRAINED_MODEL_PATH_FILE
import matplotlib.pyplot as plt

from app.etl.transformation.ml.manage_data import get_train_data
from utils.utils_ml.load_save_model import load_trained_model, save_trained_model


class PredictDifficulty:

    def __init__(self, data_lang: dict):
        self.data_lang = data_lang

    def clean_data(self, df_spark):
        # Drop nulls and duplicates
        df_spark = df_spark.dropna().drop_duplicates()

        # Drop irrelevant columns
        df_spark = (
            df_spark.select("Similarity_score", "Number_char")
            .rdd.map(lambda row: (row[0], row[1]))
            .collect()
        )

        data_array = np.array(df_spark)

        return data_array

    def create_ml(self):
        # Open csv file or create it if not exists
        data = get_train_data(self.data_lang.get("language"))

        if data.empty:
            raise Exception("Problem with generating trained data")

        # Initialize Spark session
        spark = SparkSession.builder.appName("Vocabulary").getOrCreate()
        df = spark.createDataFrame(data)

        data_array = self.clean_data(df)

        # Create pipeline
        pipeline = Pipeline(
            [("scaler", StandardScaler()), ("clusterizer", KMeans(n_clusters=3))]
        )

        param_grid = {
            "clusterizer__n_clusters": [3],
            "clusterizer__max_iter": [300, 500, 1000],
        }

        grid_search = GridSearchCV(pipeline, param_grid, cv=5)
        grid_search.fit(data_array)

        # Save the model
        save_trained_model(TRAINED_MODEL_PATH_FILE, grid_search)

        return grid_search

    def predict(self, model: Any, X):
        spark = SparkSession.builder.appName("Vocabulary").getOrCreate()
        df = spark.createDataFrame(X)

        data_array = self.clean_data(df)

        # evaluate model
        y_predict = model.predict(data_array)

        # check results
        print(silhouette_score(data_array, y_predict))

        # Return the predictions
        return y_predict

    def run(self, data_predict):  # TODO : Add type
        model = load_trained_model(TRAINED_MODEL_PATH_FILE)

        if not model:
            model = self.create_ml()

        return self.predict(model, data_predict)
