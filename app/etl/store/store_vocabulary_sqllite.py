import os
from typing import Any
import pandas
from sqlalchemy import Engine, create_engine
from app.models.german_vocabulary_table import Base
from app.models.mapping_tables_lang import MAPPING_TABLE_LANG


class StoreVocabularySqllite:

    def __init__(self, lang: str) -> None:
        self.table = MAPPING_TABLE_LANG.get(lang)

    def _create_table(self, engine: Engine):
        """Create the table in db

        Args:
            engine (Engine): engine used to create table
        """
        # Create the table if not exists
        if not engine.dialect.has_table(engine.connect(), self.table.__tablename__):
            Base.metadata.create_all(engine)
        else:
            print(f"Table {self.table.__tablename__} already exists")

    def _save_data(self, engine: Engine, df: pandas.DataFrame, table_name: Any):
        """Save data in table

        Args:
            engine (Engine): engine used to save data
            df (pandas.DataFrame): data to save
            table_name (Any): Table name
        """
        df.to_sql(table_name, con=engine, if_exists="append", index=False)

    def run(self, data: pandas.DataFrame):
        """Run the component

        Args:
            data (pandas.DataFrame): data to save

        """
        engine = create_engine(os.environ["DB_NAME"], echo=True)

        # Create the table (if not exists)
        self._create_table(engine)

        # Save the data in the table
        self._save_data(engine, data, self.table.__tablename__)

        print(f"Data saved in {self.table.__tablename__} table")
