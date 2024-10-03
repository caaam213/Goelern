import pandas
from pyspark.sql import SparkSession

from extract_vocs_from_urls import get_vocabulary

def transform_data_spark(data:pandas.DataFrame):
    spark = SparkSession.builder.appName("Vocabulary").getOrCreate()
    
    # Drop rows with missing values
    data = data.dropna()

    # Remove duplicates
    data = data.drop_duplicates()

    # Create a Spark DataFrame
    df = spark.createDataFrame(data)

    # Show the first 5 rows
    df.show(5)

first_data = get_vocabulary("https://fichesvocabulaire.com/liste-verbes-allemand-pdf")
transform_data_spark(first_data)
