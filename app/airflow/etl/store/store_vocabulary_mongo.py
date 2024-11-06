import json

import pandas
from utils.utils_mongo.words_collection import add_many_words


class StoreVocabularyMongo:

    def add_words_to_mongo(self, data: pandas.DataFrame) -> None:
        """Add words into mongo collection

        Args:
            data (pandas.DataFrame): Data to insert
        """
        data_json = data.to_json(orient="records")
        data_json = json.loads(data_json)
        add_many_words(data_json)

    def run(self, data: pandas.DataFrame):
        """Run the component

        Args:
            data (pandas.DataFrame): Data to manage
        """
        self.add_words_to_mongo(data)
