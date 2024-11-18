import json
import os
import sys
from typing import Union
from bs4 import BeautifulSoup
import requests

sys.path.insert(0, "/opt/airflow/etl")


from extraction.abstract_request import AbstractRequest

sys.path.insert(0, "/opt/")
from utils.utils_mongo.operation_mongo import add_data, find_data, remove_data
from constants.file_constants import SCRAP_URLS_PATH_FILE, SCRAP_URL_FILE_NAME
from constants.collections_constants import (
    COLLECTION_CONSTANTS,
    COLLECTION_PARAMETERS,
    FIELD_CRAWL_URL,
    FIELD_LANGUAGE,
    FIELD_SCRAP_URL,
    FIELD_STATUS,
)
from constants.status_constants import STATUS_WAITING_SCRAP
from constants.error_constants import (
    CRAWLING_VOCABULARY_NOT_FOUND,
    LANG_DICT_NOT_FOUND_ERROR,
    WEBPAGE_NOT_FOUND_ERROR,
)
from utils.utils_files.json_utils import load_json
from utils.utils_files.local_files_utils import load_data, save_data
from utils.utils_files.s3_utils import get_data_from_s3, save_data_on_s3


class CrawlingVocListUrls(AbstractRequest):
    """
    This class is responsible for extracting the urls of the vocabulary lists from the global url
    """

    def __init__(self):
        AbstractRequest.__init__(self)

    def _extract_urls_in_bs4(self, response: requests.Response) -> list:
        """Extract urls from the response using BeautifulSoup

        Args:
            response (requests.Response): Response from the global url

        Returns:
            list: List of urls of vocabulary lists to scrap
        """
        voc_list_urls = []

        soup = BeautifulSoup(response.text, "html.parser")
        # Get ol tag
        ol_tag = soup.find("ol")

        if ol_tag is None:
            return voc_list_urls

        # Get all li tags
        li_tags = ol_tag.find_all("li")

        # Get links in li tags
        for li_tag in li_tags:
            a_tag = li_tag.find("a")
            voc_list_urls.append({FIELD_SCRAP_URL: a_tag["href"]})

        return voc_list_urls

    def _get_data_from_saved_file(
        self, data_lang: dict, use_s3: bool
    ) -> Union[list, None]:
        """Get the data from the saved file

        Args:
            use_s3 (bool): True if we get the data from s3 else False

        Returns:
            Union[list, None]: List of urls of vocabulary lists to scrap or None if not found
        """

        if not use_s3:
            voc_list_urls = load_data(
                SCRAP_URLS_PATH_FILE.format(data_lang.get(FIELD_LANGUAGE))
            )
        else:
            voc_list_urls = get_data_from_s3(
                SCRAP_URL_FILE_NAME.format(data_lang.get(FIELD_LANGUAGE))
            )

            if voc_list_urls:
                voc_list_urls = voc_list_urls.decode("utf-8").replace("'", '"')

        json_list_urls = load_json(voc_list_urls)
        return json_list_urls

    def _save_data(self, data_lang: dict, data: list, use_s3: bool) -> None:
        """Save the data on S3 or locally

        Args:
            data_lang (dict): data related to the language
            data (list): List of urls of vocabulary lists to scrap
            use_s3 (bool): True if we save the data on s3 else False
        """
        if not use_s3:
            save_data(
                SCRAP_URLS_PATH_FILE.format(data_lang.get(FIELD_LANGUAGE)),
                json.dumps(data),
            )
        else:
            save_data_on_s3(
                SCRAP_URL_FILE_NAME.format(data_lang.get(FIELD_LANGUAGE)), data
            )

    def run(self, mongo_hook, lang: str):
        """
            Run the component to extract the urls of the vocabulary lists from the global url
        Args:
            data_lang (str): language of the data to scrap
        """
        data_lang = find_data(
            mongo_hook,
            os.environ["MONGO_DB_DEV"],
            COLLECTION_CONSTANTS,
            {FIELD_LANGUAGE: lang},
            True,
        )
        if not data_lang:
            raise ValueError(LANG_DICT_NOT_FOUND_ERROR.format(lang))

        # Get data if not saved
        crawling_url = data_lang.get(FIELD_CRAWL_URL)
        response = self.get_data(crawling_url)
        if not response:
            raise ValueError(WEBPAGE_NOT_FOUND_ERROR.format(crawling_url))

        voc_list_urls = self._extract_urls_in_bs4(response)

        if not voc_list_urls:
            raise Exception(CRAWLING_VOCABULARY_NOT_FOUND)

        # Format the data for mongo
        voc_list_urls = [
            {
                FIELD_SCRAP_URL: voc_list_url[FIELD_SCRAP_URL],
                FIELD_LANGUAGE: lang,
                FIELD_STATUS: STATUS_WAITING_SCRAP,
            }
            for voc_list_url in voc_list_urls
        ]

        # Remove the former data and add the new data
        remove_data(
            mongo_hook,
            os.environ["MONGO_DB_DEV"],
            COLLECTION_PARAMETERS,
            {FIELD_LANGUAGE: lang},
        )
        add_data(
            mongo_hook, os.environ["MONGO_DB_DEV"], COLLECTION_PARAMETERS, voc_list_urls
        )
