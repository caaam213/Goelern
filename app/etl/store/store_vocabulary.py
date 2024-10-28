import pandas
from app.etl.store.store_vocabulary_mongo import StoreVocabularyMongo
from app.etl.store.store_vocabulary_sqllite import StoreVocabularySqllite


class StoreVocabulary:

    def __init__(self, lang: str) -> None:
        self.lang = lang

    def run(self, data: pandas.DataFrame):
        """Run mongo and sqllite storages

        Args:
            data (pandas.Dataframe): Data to save
        """
        # Mongo storage
        mongo_storage = StoreVocabularyMongo()
        mongo_storage.run(data)

        # SQLLite storage
        sqllite_storage = StoreVocabularySqllite(self.lang)
        sqllite_storage.run(data)
