import os
import sys
from sqlalchemy import create_engine
from dotenv import load_dotenv

load_dotenv()

models_path = os.getenv("MODELS_PATH")

if models_path:
    sys.path.append(models_path)

from app.process_vocabulary import ProcessVocabulary
from app.scrap_vocabulary import ScrapVocabulary
from models.german_vocabulary_table import Base, GermanVocabularyTable


class StoreVocabulary:

    def __init__(self, df_pandas):
        self.df_pandas = df_pandas

    def _create_table(self, engine):
        # Create the table if not exists
        if not engine.dialect.has_table(engine.connect(), GermanVocabularyTable.__tablename__):
            Base.metadata.create_all(engine)
        else:
            print(f"Table {GermanVocabularyTable.__tablename__} already exists")

    def _save_data(self, engine, df, table_name):
        df.to_sql(table_name, con=engine, if_exists="replace", index=False)

    def run(self):
        engine = create_engine(os.environ["DB_NAME"], echo=True)

        # Create the table (if not exists)
        self._create_table(engine)

        # Save the data in the table
        self._save_data(engine, self.df_pandas, GermanVocabularyTable.__tablename__)

        print(f"Data saved in {GermanVocabularyTable.__tablename__} table")

        return True


first_data = ScrapVocabulary(
    "https://fichesvocabulaire.com/liste-verbes-allemand-pdf-action-mouvements"
).run()
data = ProcessVocabulary(first_data).run()
StoreVocabulary(data).run()
