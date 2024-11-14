from datetime import datetime
from io import StringIO
import logging
import os
import sys
from typing import Any



import numpy as np
import pandas as pd
from pyspark.sql import SparkSession
from sklearn.cluster import KMeans
from sklearn.metrics import silhouette_score
from sklearn.model_selection import GridSearchCV
from sklearn.pipeline import Pipeline
from sklearn.preprocessing import StandardScaler
from airflow.providers.mongo.hooks.mongo import MongoHook



sys.path.insert(0, "/opt/airflow/")
from etl.ml.manage_data import get_train_data

sys.path.insert(0, "/opt/")
from utils.utils_ml.load_save_model import load_trained_model, save_trained_model
from utils.utils_mongo.operation_mongo import find_data, update_data
from utils.utils_files.s3_utils import get_data_from_s3, save_data_on_s3
from constants.s3_constants import POSTPROCESSED_DATA, PREPROCESSED_DATA, S3_BUCKET
from constants.file_constants import TRAINED_MODEL_PATH_FILE

class PredictDifficulty:

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

    def create_ml(self, data_lang: dict):
        # Load training data
        data = get_train_data(data_lang.get("language"))
        
        if data.empty:
            raise Exception("Problem with generating training data")

        # Initialize Spark session and create DataFrame
        spark = SparkSession.builder.appName("Vocabulary").getOrCreate()
        df = spark.createDataFrame(data)

        # Convert Spark DataFrame to Pandas DataFrame
        data_pandas = df.toPandas()
        
        # Clean and prepare data for ML pipeline
        data_array = self.clean_data(data_pandas)  # Assumes this returns numpy array or pandas DataFrame

        # Define scikit-learn pipeline
        pipeline = Pipeline([
            ("scaler", StandardScaler()), 
            ("clusterizer", KMeans(n_clusters=3))
        ])

        # Set up grid search parameters
        param_grid = {
            "clusterizer__n_clusters": [3],
            "clusterizer__max_iter": [300, 500, 1000],
        }
        
        # Initialize and fit GridSearchCV
        grid_search = GridSearchCV(pipeline, param_grid, cv=5)
        grid_search.fit(data_array)

        # Save the model
        save_trained_model(TRAINED_MODEL_PATH_FILE, grid_search)

        return grid_search.best_estimator_
    def predict(self, model: Any, X):
        spark = SparkSession.builder.appName("Vocabulary").getOrCreate()
        df = spark.createDataFrame(X)

        data_array = self.clean_data(df)

        # evaluate model
        y_predict = model.predict(data_array)

        # check results
        logging.info(silhouette_score(data_array, y_predict))

        # Return the predictions
        return y_predict

    def run(self, mongo_hook : MongoHook):
        
        date = datetime.now().strftime("%Y%m%d")
        file_info = find_data(mongo_hook, os.environ["MONGO_DB_DEV"], "raw_files", {"status": "WAITING FOR AI PROCESSING", "created_date":date}, True)
        
        if not file_info:
            logging.error("No file info found")
            return None
        
        # Get info
        file_name = f'{PREPROCESSED_DATA}/{file_info.get("file_name")}'
        lang = file_info.get("lang")
        
        # TODO :  Factorise this function
        data_lang = find_data(mongo_hook, os.environ["MONGO_DB_DEV"], "constants", {"language": lang}, True)
        logging.info(data_lang)
        
        # If data_lang is empty, return an empty list
        if not data_lang:
            logging.error("No data found for the language")
            return []
        
        file_content = get_data_from_s3(S3_BUCKET, file_name)
        
        if not file_content:
            logging.error("No data found in the file")
            return None
        
        file_content = file_content.decode('utf-8')
        csv_data = StringIO(file_content)
        
        # Load into pandas DataFrame
        data_predict = pd.read_csv(csv_data)
    
        model = load_trained_model(TRAINED_MODEL_PATH_FILE)

        if not model:
            model = self.create_ml(data_lang)
        else:
            model = model.best_estimator_

        logging.info(type(data_predict))
        predictions = self.predict(model, data_predict.copy())
        
        if hasattr(model, 'best_estimator_'):
            kmeans_model = model.best_estimator_.named_steps["clusterizer"]
        else:
            kmeans_model = model.named_steps["clusterizer"]
            
        centers = kmeans_model.cluster_centers_
        logging.info(f"Cluster centers: {centers}")

        # Trier les centres des clusters en fonction de la somme de leurs valeurs (ou une autre métrique de votre choix)
        center_sums = np.sum(centers, axis=1)  # Calculer la somme des coordonnées pour chaque centre
        sorted_indices = np.argsort(center_sums)  # Trier les indices des centres en fonction de la somme

        logging.info(f"Sorted cluster indices based on sum of coordinates: {sorted_indices}")

        # Mappez les indices triés à des labels de difficulté
        difficulty_labels = {sorted_indices[2]: "easy", sorted_indices[1]: "medium", sorted_indices[0]: "difficult"}

        logging.info(f"Difficulty levels mapping: {difficulty_labels}")

        # Mapper les prédictions aux niveaux de difficulté
        mapped_difficulties = [difficulty_labels[p] for p in predictions]

        # Ajouter la colonne 'difficulty' dans data_predict
        data_predict["difficulty_label"] = mapped_difficulties

        # Vérifier les résultats
        logging.info(f"Difficulty labels applied: {data_predict['difficulty_label']}")
        logging.info(data_predict[["difficulty_label"]].head())
        
        # Save the predictions to s3
        date = datetime.now().strftime("%Y%m%d")
        save_data_on_s3(S3_BUCKET, f"{POSTPROCESSED_DATA}/goelern_{date}.csv", data_predict.to_csv(index=False))
        
        # Add the file name to mongodb
        update_data(mongo_hook, os.environ["MONGO_DB_DEV"], "raw_files", {"file_name": f"goelern_{date}.csv"}, {"file_name": f"goelern_{date}.csv","status": "FINISHED", "lang":lang, "created_date":date}, True)
        
       
        
