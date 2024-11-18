from datetime import datetime
from io import StringIO
import os
import sys
from typing import Union
import pandas


sys.path.insert(0, "/opt/")
from constants.error_constants import (
    LANG_DICT_NOT_FOUND_ERROR,
    MONGO_DATA_NOT_FOUND,
    S3_DATA_NOT_FOUND,
)
from constants.status_constants import (
    STATUS_WAITING_AI_PROCESSING,
    STATUS_WAITING_PROCESSING,
)
from constants.collections_constants import (
    COLLECTION_CONSTANTS,
    COLLECTION_PARAMETERS,
    FIELD_BASE_LANGUAGE,
    FIELD_CREATED_DATE,
    FIELD_FILE_NAME,
    FIELD_FREQUENCY,
    FIELD_LANGUAGE,
    FIELD_NUMBER_CHARS,
    FIELD_SCRAP_URL,
    FIELD_SIMILARITY_SCORE,
    FIELD_STATUS,
    FIELD_TRANSLATION,
)
from constants.s3_constants import PREPROCESSED_DATA, RAW_DATA, S3_BUCKET
from utils.utils_files.s3_utils import get_data_from_s3, save_data_on_s3
from utils.utils_mongo.operation_mongo import find_data, update_data
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
            The score is between 0 and 1, where 0 means the words are identical and 1 means the words are different.
        """
        return 1 - SequenceMatcher(None, word, translation).ratio()

    def _get_word_frequency(self, word: str, lang) -> float:
        """Get the frequency of a word in a language

        Args:
            word (str): Word in a language
            lang (str): Language of the word

        Returns:
            float: Frequency of the word in the language
        """
        return word_frequency(word, lang)

    def _process_w_spark(
        self, df: pandas.DataFrame, lang: str, data_lang: dict
    ) -> None:
        """Process the data with Spark and return a DataFrame with additional information

        Args:
            df (pandas.DataFrame): DataFrame with the words
            lang (str): Language of the words
            data_lang (dict): Data for the language
        """
        base_name_col = data_lang.get(FIELD_BASE_LANGUAGE)
        trans_name_col = data_lang.get(FIELD_TRANSLATION)

        spark = SparkSession.builder.appName("Vocabulary").getOrCreate()

        # Drop rows with missing values
        df = df.dropna()

        # Remove duplicates
        df = df.drop_duplicates()

        df_spark = spark.createDataFrame(df)

        # For the word, get the number of characters
        df_spark = df_spark.withColumn(
            FIELD_NUMBER_CHARS, length(df_spark[base_name_col])
        )

        # Get the frequency of the word
        get_word_frequency_udf = udf(
            partial(self._get_word_frequency, lang=lang), FloatType()
        )
        df_spark = df_spark.withColumn(
            FIELD_FREQUENCY, get_word_frequency_udf(df_spark[base_name_col])
        )

        # Calculate the similarity score
        get_word_frequency_udf = udf(self._get_similarity_score, FloatType())
        df_spark = df_spark.withColumn(
            FIELD_SIMILARITY_SCORE,
            get_word_frequency_udf(df_spark[base_name_col], df_spark[trans_name_col]),
        )

        # Convert the DataFrame to a pandas DataFrame
        df_pandas = df_spark.toPandas()

        return df_pandas

    def run(self, mongo_hook: MongoHook) -> Union[None, pandas.DataFrame]:
        """Run the component

        Args:
            mongo_hook (MongoHook): MongoHook object
        """
        date = datetime.now().strftime("%Y-%m-%d")
        file_info = find_data(
            mongo_hook,
            os.environ["MONGO_DB_DEV"],
            COLLECTION_PARAMETERS,
            {FIELD_STATUS: STATUS_WAITING_PROCESSING, FIELD_CREATED_DATE: date},
            True,
        )

        if not file_info:
            raise ValueError(MONGO_DATA_NOT_FOUND)

        # Get info
        file_name = file_info.get(FIELD_FILE_NAME)
        raw_file_name = f"{RAW_DATA}/{file_name}"
        ext_file_name = f"{PREPROCESSED_DATA}/{file_name}"
        lang = file_info.get(FIELD_LANGUAGE)
        scrap_url = file_info.get(FIELD_SCRAP_URL)

        data_lang = find_data(
            mongo_hook,
            os.environ["MONGO_DB_DEV"],
            COLLECTION_CONSTANTS,
            {FIELD_LANGUAGE: lang},
            True,
        )

        if not data_lang:
            raise Exception(LANG_DICT_NOT_FOUND_ERROR.format(lang))

        # Extract data from s3
        file_content = get_data_from_s3(S3_BUCKET, raw_file_name)

        if not file_content:
            raise Exception(S3_DATA_NOT_FOUND)

        file_content = file_content.decode("utf-8")
        csv_data = StringIO(file_content)

        # Process the data
        df = pandas.read_csv(csv_data)
        df_pandas = self._process_w_spark(df, lang, data_lang)

        # Change the status of the data
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
                FIELD_STATUS: STATUS_WAITING_AI_PROCESSING,
                FIELD_LANGUAGE: lang,
                FIELD_CREATED_DATE: date,
                FIELD_SCRAP_URL: scrap_url,
            },
        )

        # Save the data on S3
        save_data_on_s3(
            S3_BUCKET,
            ext_file_name,
            df_pandas.to_csv(index=False),
        )

        return df_pandas
