from typing import Union
import pandas as pd
from app.constants.execute_constants import DICT_LIST_URLS
from app.constants.file_constants import VOCABULARY_FILE
from app.etl.extraction.scrap_vocabulary import ScrapVocabulary
from app.etl.transformation.process_vocabulary import ProcessVocabulary
from utils.utils_files.local_files_utils import load_csv, load_data
from utils.utils_mongo.constants_collection import get_data_by_lang


def get_train_data(lang: str) -> Union[pd.DataFrame, None]:
    """
    Open an existing data if the file exists or generate data to train a model and save it
    Args:
        lang (str): Targetted language

    Returns:
        pd.DataFrame: Data to train a model in pandas dataframe format or None if lang is not valid
    """

    # Verify if the file doesnt exist
    data = load_csv(VOCABULARY_FILE.format(lang))

    if data is not None:
        return data

    # Verify if the lang is valid
    urls_list = DICT_LIST_URLS.get(lang, None)
    print(urls_list)

    if not urls_list:
        print("Lang is incorrect")
        return None

    df = pd.DataFrame()
    data_lang = get_data_by_lang(lang)  # No need to verify if data_lang is empty

    # Get and transform data
    for url in urls_list:
        print(f"Getting data from {url}")
        scraping_obj = ScrapVocabulary()
        scraped_data = scraping_obj.run(scrap_url=url)

        print("Process data")
        processed_data = ProcessVocabulary(data_lang).run(scraped_data)

        df = pd.concat([df, processed_data])

    # Save the data
    print("Save the data")
    df.to_csv(VOCABULARY_FILE.format(lang), index=False)
    print(df)
    return df
