import os
import pandas
from pyspark.sql import SparkSession
from pyspark.sql.functions import length
from pyspark.sql.functions import udf
from pyspark.sql.types import FloatType

from extract_vocs_from_urls import get_vocabulary
from difflib import SequenceMatcher
from wordfreq import word_frequency

def get_similarity_score(word: str, translation: str) -> float:
    """Calculate the similarity score between a word and its translation

    Args:
        word (str): Word in a wanted language (for now, German)
        translation (str): Translation of the word in another language (for now, French)

    Returns:
        float: Similarity score between the word and its translation.
        The score is between 0 and 1, where 0 means the words are identical and 1 means the words are different.
    """
    return 1 - SequenceMatcher(None, word, translation).ratio()


def get_word_frequency(word: str) -> float:
    """Get the frequency of a word in a language

    Args:
        word (str): Word in a language

    Returns:
        float: Frequency of the word in the language
    """
    return word_frequency(word, "de")

def store_data(data): # TODO : Move it to the storing part
    df_to_save = data.orderBy(data["frequency"].desc()).select(
        "French_word", "German_word"
    )
    category_value = data.select("Category").first()[0]
    output_file = f'{os.environ["DATA_PATH"]}/{category_value}_vocabulary.txt'
    print(f"Saving the data to {output_file}")

    df_to_save.toPandas().to_csv(
        output_file,
        sep="\t",
        index=False,
        header=False
    )


def transform_data_spark(data: list):

    # Create a DataFrame from the list
    df = pandas.DataFrame(
        data, columns=["French_word", "German_word", "Category"], index=None
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
    get_word_frequency_udf = udf(get_word_frequency, FloatType())
    df_spark = df_spark.withColumn(
        "frequency", get_word_frequency_udf(df_spark["German_word"])
    )

    # Store only the french and german words for anki purposes in .txt format
    store_data(df_spark)

    # Calculate the similarity score between the German and French words
    get_word_frequency_udf = udf(get_similarity_score, FloatType())
    df_spark = df_spark.withColumn(
        "Similarity_score",
        get_word_frequency_udf(df_spark["German_word"], df_spark["French_word"]),
    )

    # Show the data with the highest frequencies
    df_spark.show(5)

    return df_spark


first_data = get_vocabulary(
    "https://fichesvocabulaire.com/liste-verbes-allemand-pdf-action-mouvements"
)
transform_data_spark(first_data)
