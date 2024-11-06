from typing import Union
import pandas
from pyspark.sql import SparkSession
from pyspark.sql.functions import length
from pyspark.sql.functions import udf
from pyspark.sql.types import FloatType

from difflib import SequenceMatcher
from wordfreq import word_frequency


class ProcessVocabulary:

    def __init__(self, data_lang: dict):
        self.data_lang = data_lang

    def _get_similarity_score(self, word: str, translation: str) -> float:
        """Calculate the similarity score between a word and its translation

        Args:
            word (str): Word in a wanted language
            translation (str): Translation of the word in another language (French)

        Returns:
            float: Similarity score between the word and its translation.
            The score is between 0 and 1, where 1 means the words are identical and 0 means the words are different.
        """
        return 1 - SequenceMatcher(None, word, translation).ratio()

    def _get_word_frequency(self, word: str) -> float:
        """Get the frequency of a word in a language

        Args:
            word (str): Word in a language

        Returns:
            float: Frequency of the word in the language
        """
        return word_frequency(word, self.data_lang.get("language"))

    def run(self, data: list) -> Union[None, pandas.DataFrame]:
        """Run the component

        Args:
            data (list): List of words

        Returns:
            Union[None, pandas.DataFrame]: Dataframe with additional information or None if something went wrong
        """

        base_name_col = self.data_lang.get("base_name_col")
        trans_name_col = self.data_lang.get("translate_name_col")

        # Verify if data lang is not empty
        if not self.data_lang:
            return None

        # Create a DataFrame from the list
        df = pandas.DataFrame(
            data,
            columns=[
                base_name_col,
                trans_name_col,
                "Category",
            ],
            index=None,
        )

        spark = SparkSession.builder.appName("Vocabulary").getOrCreate()

        # Drop rows with missing values
        df = df.dropna()

        # Remove duplicates
        df = df.drop_duplicates()

        df_spark = spark.createDataFrame(df)

        # For the word, get the number of characters
        df_spark = df_spark.withColumn("Number_char", length(df_spark[base_name_col]))

        # Get the frequency of the word
        get_word_frequency_udf = udf(self._get_word_frequency, FloatType())
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

        return df_pandas
