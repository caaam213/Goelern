import pandas
from pyspark.sql import SparkSession
from pyspark.sql.functions import length
from pyspark.sql.functions import udf
from pyspark.sql.types import FloatType

from difflib import SequenceMatcher
from wordfreq import word_frequency


class ProcessVocabulary:

    def __init__(self, data: list):
        self.data = data

    def _get_similarity_score(self, word: str, translation: str) -> float:
        """Calculate the similarity score between a word and its translation

        Args:
            word (str): Word in a wanted language (for now, German)
            translation (str): Translation of the word in another language (for now, French)

        Returns:
            float: Similarity score between the word and its translation.
            The score is between 0 and 1, where 1 means the words are identical and 0 means the words are different.
        """
        return SequenceMatcher(None, word, translation).ratio()

    def _get_word_frequency(self, word: str) -> float:
        """Get the frequency of a word in a language

        Args:
            word (str): Word in a language

        Returns:
            float: Frequency of the word in the language
        """
        return word_frequency(word, "de")

    def _store_data(self, data):  # TODO : Move it to the storing part
        df_to_save = data.orderBy(data["frequency"].desc()).select(
            "French_word", "German_word"
        )
        category_value = data.select("Category").first()[0]
        output_file = f"/data/{category_value}_vocabulary.txt"
        print(f"Saving the data to {output_file}")

        df_to_save.toPandas().to_csv(output_file, sep="\t", index=False, header=False)

    def run(self):

        # Create a DataFrame from the list
        df = pandas.DataFrame(
            self.data, columns=["French_word", "German_word", "Category"], index=None
        )

        spark = SparkSession.builder.appName("Vocabulary").getOrCreate()

        # Drop rows with missing values
        df = df.dropna()

        # Remove duplicates
        df = df.drop_duplicates()

        df_spark = spark.createDataFrame(df)

        # For the German word, get the number of characters
        df_spark = df_spark.withColumn("number_char", length(df_spark["German_word"]))

        # Get the frequency of the German word
        get_word_frequency_udf = udf(self._get_word_frequency, FloatType())
        df_spark = df_spark.withColumn(
            "frequency", get_word_frequency_udf(df_spark["German_word"])
        )

        # Calculate the similarity score between the German and French words
        get_word_frequency_udf = udf(self._get_similarity_score, FloatType())
        df_spark = df_spark.withColumn(
            "Similarity_score",
            get_word_frequency_udf(df_spark["German_word"], df_spark["French_word"]),
        )

        # Show the data with the highest frequencies
        df_spark.show(5)
        
        # Store the data
        self._store_data(df_spark)

        # Convert the DataFrame to a pandas DataFrame
        df_pandas = df_spark.toPandas()

        return df_pandas