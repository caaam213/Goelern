import os
import pandas as pd
from sqlalchemy import create_engine, select, text
from pyspark.sql import SparkSession

from constants.constants import ALL, ALPHABETICAL_ORDER_STR, ASC_STR, DESC_STR, FREQUENCY_STR, NO_SORT_STR


class VocabService:
    def __init__(self):
        self.df_spark = None
    

    def get_all_vocabs(self):
        
        table_name = "german_vocabulary"
                
        try:
            engine = create_engine(os.environ["DB_NAME"], echo=True)
            
            query = text(f"SELECT * FROM {table_name}")
            
            df_pandas = pd.read_sql(query, con=engine)
            
            spark = SparkSession.builder \
            .appName("Pandas to PySpark to make queries") \
            .getOrCreate()
            
            self.df_spark = spark.createDataFrame(df_pandas)

        except Exception as e:
            print(f"Error: {e}")
    
    
    def sort(self, category:str, order_field:str, order_type:str):
        # Filter by category
        df_spark = self.df_spark.select("*")
        print(df_spark == self.df_spark)
        print(df_spark)
        if category != ALL:
            category = category.replace(" ", "_")
            df_spark = self.filter_by_category(df_spark, category)
        
        sort_asc = True if order_type == ASC_STR else False
        
        # Sort the data
        if order_field == ALPHABETICAL_ORDER_STR:
            df_spark = self.order_by_field(df_spark, "German_word", sort_asc)
        elif order_field == FREQUENCY_STR:
            df_spark = self.order_by_field(df_spark, "frequency", sort_asc)
        
        # Get only words and their translations
        df_spark = df_spark.select("German_word", "French_word")
        
        # Rename columns in one line
        df_spark = df_spark.toDF("German word", "French word")
        
        return df_spark
        
    
    def order_by_field(self, df, column_name: str, order_type: str):
        return df.orderBy(column_name, ascending=order_type)
    
    def filter_by_category(self, df, category: str):
        return df.filter(self.df_spark["Category"] == category)
    
    def get_category_list(self):
        category_list = self.df_spark.select("Category").distinct().collect()
        category_list = [category.Category.replace("_", " ") for category in category_list]
        return category_list
    

vocab_service = VocabService()
vocab_service.get_all_vocabs()
print(vocab_service.get_category_list())