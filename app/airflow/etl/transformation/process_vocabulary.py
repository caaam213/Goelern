from datetime import datetime
from io import StringIO
import logging
import os
import sys
from typing import Union
import pandas

sys.path.insert(0, "/opt/")
from constants.s3_constants import PREPROCESSED_DATA, RAW_DATA, S3_BUCKET
from utils.utils_files.s3_utils import get_data_from_s3, save_data_on_s3
from utils.utils_mongo.operation_mongo import add_data, find_data, update_data
from pyspark.sql import SparkSession
from pyspark.sql.functions import length
from pyspark.sql.functions import udf
from functools import partial
from pyspark.sql.types import FloatType

from difflib import SequenceMatcher
from wordfreq import word_frequency
from airflow.providers.mongo.hooks.mongo import MongoHook


class ProcessVocabulary:

    def _get_similarity_score(self, word: str, translation: str) -> float:
        """Calculate the similarity score between a word and its translation

        Args:
            word (str): Word in a wanted language
            translation (str): Translation of the word in another language (French)

        Returns:
            float: Similarity score between the word and its translation.
            The score is between 0 and 1, where 1 means the words are identical and 0 means the words are different.
        """
        return 1-SequenceMatcher(None, word, translation).ratio()

    def _get_word_frequency(self, word: str, lang) -> float:
        """Get the frequency of a word in a language

        Args:
            word (str): Word in a language
            lang (str): Language of the word

        Returns:
            float: Frequency of the word in the language
        """
        return word_frequency(word, lang)

    def run(self, mongo_hook: MongoHook) -> Union[None, pandas.DataFrame]:
        """Run the component

        Args:
            data (list): List of words

        Returns:
            Union[None, pandas.DataFrame]: Dataframe with additional information or None if something went wrong
        """
        date = datetime.now().strftime("%Y%m%d")
        file_info = find_data(mongo_hook, os.environ["MONGO_DB_DEV"], "raw_files", {"status": "WAITING FOR PROCESSING", "created_date":date}, True)
        
        if not file_info:
            logging.error("No file info found")
            return None
        
        # Get info
        file_name = file_info.get("file_name")
        lang = file_info.get("lang")
        scrap_url = file_info.get("scrap_url")
        
        logging.info(f"File name: {file_name}, Language: {lang}, Scrap URL: {scrap_url}")
            
        data_lang = find_data(mongo_hook, os.environ["MONGO_DB_DEV"], "constants", {"language": lang}, True)
        
        if not data_lang:
            logging.error("No data found for the language")
            return None
        
        # Extract data from s3
        file_content = get_data_from_s3(S3_BUCKET, f"{RAW_DATA}/{file_name}")
        
        if not file_content:
            logging.error("No data found in the file")
            return None
        
        base_name_col = data_lang.get("base_name_col")
        trans_name_col = data_lang.get("trans_name_col")
        
        logging.info(f"Base name col: {base_name_col}, Translation name : {trans_name_col}")

        file_content = file_content.decode('utf-8')
        csv_data = StringIO(file_content)

        # Load into pandas DataFrame
        df = pandas.read_csv(csv_data)

        spark = SparkSession.builder.appName("Vocabulary").getOrCreate()

        # Drop rows with missing values
        df = df.dropna()

        # Remove duplicates
        df = df.drop_duplicates()

        df_spark = spark.createDataFrame(df)

        # For the word, get the number of characters
        df_spark = df_spark.withColumn("Number_char", length(df_spark[base_name_col]))

        # Get the frequency of the word
        get_word_frequency_udf = udf(partial(self._get_word_frequency, lang=lang), FloatType())
        df_spark = df_spark.withColumn(
            "Frequency", get_word_frequency_udf(df_spark[base_name_col])
        )

        # Calculate the similarity score
        get_word_frequency_udf = udf(self._get_similarity_score, FloatType())
        df_spark = df_spark.withColumn(
            "Similarity_score",
            get_word_frequency_udf(df_spark[base_name_col], df_spark[trans_name_col]),
        )

        # Convert the DataFrame to a pandas DataFrame
        df_pandas = df_spark.toPandas()
        
        # Change the status of the data
        update_data(mongo_hook, os.environ["MONGO_DB_DEV"], "parameters",{"scrap_url": scrap_url, "language":lang} ,{"status": "WAITING FOR AI PROCESSING"})
        
        # Save the data on S3
        date = datetime.now().strftime("%Y%m%d")
        save_data_on_s3(S3_BUCKET, f"{PREPROCESSED_DATA}/goelern_{date}.csv", df_pandas.to_csv(index=False))
        
    
        # path_csv_file = f"/opt/airflow/data/goelern_{date}_8.csv"
        # df_pandas.to_csv(path_csv_file, index=False)
        
        # Add the file name to mongodb
        update_data(mongo_hook, os.environ["MONGO_DB_DEV"], "raw_files", {"file_name": f"goelern_{date}.csv", "scrap_url":scrap_url},{"status": "WAITING FOR AI PROCESSING", "lang":lang, "created_date":date, "scrap_url":scrap_url}, True)
        
        return df_pandas
