from datetime import datetime
import os
import re
import sys
from bs4 import BeautifulSoup
import requests
import pandas as pd
from airflow.providers.mongo.hooks.mongo import MongoHook


sys.path.insert(0, "/opt/airflow/etl")


from extraction.abstract_request import AbstractRequest

sys.path.insert(0, "/opt/")
from utils.utils_files.s3_utils import save_data_on_s3
from utils.utils_mongo.operation_mongo import add_data, find_data, update_data
from utils.utils_url import get_last_path_segment
from constants.status_constants import STATUS_WAITING_PROCESSING
from constants.collections_constants import (
    COLLECTION_CONSTANTS,
    COLLECTION_PARAMETERS,
    FIELD_BASE_LANGUAGE,
    FIELD_CATEGORY,
    FIELD_CREATED_DATE,
    FIELD_FILE_NAME,
    FIELD_LANGUAGE,
    FIELD_SCRAP_URL,
    FIELD_STATUS,
    FIELD_TRANSLATION,
)
from constants.error_constants import (
    CATEGORY_NOT_FOUND,
    LANG_DICT_NOT_FOUND_ERROR,
    URL_FORMAT_NOT_VALID,
    WEBPAGE_NOT_FOUND_ERROR,
)
from constants.s3_constants import RAW_DATA, S3_BUCKET


class ScrapVocabulary(AbstractRequest):

    def __init__(self):
        AbstractRequest.__init__(self)
        self.pattern = r"^https?:\/\/fichesvocabulaire\.com\/.*"

    def _get_category(self, scrap_url: str) -> str:
        """Get category name for the vocabulary list in the url

        Args:
            scrap_url (str): Url of the vocabulary list

        Returns:
            str: Category name
        """
        name = get_last_path_segment(scrap_url)

        if not name:
            return ""

        name = name.replace("-pdf", "").replace("-", "_")

        return name

    def _extract_urls_in_bs4(self, response: requests.Response, category: str) -> list:
        """Extract vocabulary words from the response using BeautifulSoup

        Args:
            response (requests.Response): Response from the vocabulary list url request
            category (str): Category name of the vocabulary list

        Returns:
            list: List of vocabulary words with their translations in French
        """

        soup = BeautifulSoup(response.text, "html.parser")
        vocabulary_list = []

        # Get table tag
        table_tag = soup.find("table")

        if not table_tag:
            return vocabulary_list

        tr_tags = table_tag.find_all("tr")

        # Remove td which not contains align="LEFT" because they are not words
        tr_tags = [tr_tag for tr_tag in tr_tags if tr_tag.find("td")["align"] == "LEFT"]

        # First td is the French word, second td is the word in the targeted language
        for tr_tag in tr_tags:
            td_tags = tr_tag.find_all("td")
            vocabulary_list.append([td_tags[0].text, td_tags[1].text, category])

        return vocabulary_list

    def _verify_scrap_url(self, scrap_url: str) -> bool:
        """Verify if the url is valid using regex

        Args:
            scrap_url (str): url to verify

        Returns:
            bool: True if the url is valid else False
        """
        return bool(re.match(self.pattern, scrap_url))

    def _save_data(
        self,
        mongo_hook: MongoHook,
        df_vocabularies: pd.DataFrame,
        lang: str,
        scrap_url: str,
        category: str,
    ) -> None:
        """Save the data on S3 and add the file name to MongoDB

        Args:
            mongo_hook (MongoHook): Airflow hook for MongoDB connection
            df_vocabularies (pd.DataFrame): Data to save
            lang (str): Language of the vocabulary list
            scrap_url (str): Url of the vocabulary list
            category (str): Category name of the vocabulary list
        """
        # Get the date
        date = datetime.now().strftime("%Y-%m-%d")

        # Save the data on S3
        file_name = f"goelern_{category}_{date}.csv"
        ext_file_name = f"{RAW_DATA}/{file_name}"
        save_data_on_s3(S3_BUCKET, ext_file_name, df_vocabularies.to_csv(index=False))

        # Change the status of the data
        update_data(
            mongo_hook,
            os.environ["MONGO_DB_DEV"],
            COLLECTION_PARAMETERS,
            {FIELD_SCRAP_URL: scrap_url, FIELD_LANGUAGE: lang},
            {
                FIELD_FILE_NAME: file_name,
                FIELD_STATUS: STATUS_WAITING_PROCESSING,
                FIELD_LANGUAGE: lang,
                FIELD_CREATED_DATE: date,
                FIELD_SCRAP_URL: scrap_url,
            },
        )

    def run(self, mongo_hook: MongoHook, lang: str, scrap_url: str):
        """Run the component to scrap the vocabulary list

        Args:
            mongo_hook (MongoHook): Airflow hook for MongoDB connection
            lang (str): Language of the vocabulary list
            scrap_url (str): Url of the vocabulary list
        """
        data_lang = find_data(
            mongo_hook,
            os.environ["MONGO_DB_DEV"],
            COLLECTION_CONSTANTS,
            {FIELD_LANGUAGE: lang},
            True,
        )

        # If data_lang is empty, return an empty list
        if not data_lang:
            raise ValueError(LANG_DICT_NOT_FOUND_ERROR.format(lang))

        base_name_col = data_lang.get(FIELD_BASE_LANGUAGE)
        trans_name_col = data_lang.get(FIELD_TRANSLATION)

        # Verify if url is valid
        if not self._verify_scrap_url(scrap_url):
            raise ValueError(URL_FORMAT_NOT_VALID.format(scrap_url))

        # Get title of the page
        category = self._get_category(scrap_url)
        if not category:
            raise ValueError(CATEGORY_NOT_FOUND)

        response = self.get_data(scrap_url)
        if not response:
            raise ValueError(WEBPAGE_NOT_FOUND_ERROR.format(scrap_url))

        # Extract vocabulary list
        vocabulary_list = self._extract_urls_in_bs4(response, category)
        df_vocabularies = pd.DataFrame(
            vocabulary_list,
            columns=[
                trans_name_col,
                base_name_col,
                FIELD_CATEGORY,
            ],
        )

        # Save the data
        self._save_data(mongo_hook, df_vocabularies, lang, scrap_url, category)
