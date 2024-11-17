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
from utils.utils_files.json_utils import load_json
from utils.utils_files.local_files_utils import load_data, save_data
from utils.utils_files.s3_utils import get_data_from_s3, save_data_on_s3

class CrawlingVocListUrls(AbstractRequest):
    """
    This class is responsible for extracting the urls of the vocabulary lists from the global url
    """

    def __init__(self):
        AbstractRequest.__init__(self)

    def _extract_urls_in_bs4(self, response:requests.Response) -> list:
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
            print("No li tags found")
            return []

        # Get all li tags
        li_tags = ol_tag.find_all("li")

        # Get links in li tags
        for li_tag in li_tags:
            a_tag = li_tag.find("a")
            voc_list_urls.append({"scrap_url": a_tag["href"]})

        return voc_list_urls

    def _get_data_from_saved_file(self, data_lang:dict, use_s3: bool) -> Union[list, None]:
        """Get the data from the saved file

        Args:
            use_s3 (bool): True if we get the data from s3 else False

        Returns:
            Union[list, None]: List of urls of vocabulary lists to scrap or None if not found
        """

        if not use_s3:
            voc_list_urls = load_data(
                SCRAP_URLS_PATH_FILE.format(data_lang.get("language"))
            )
        else:
            voc_list_urls = get_data_from_s3(
                SCRAP_URL_FILE_NAME.format(data_lang.get("language"))
            )

            if voc_list_urls:
                voc_list_urls = voc_list_urls.decode("utf-8").replace("'", '"')

        json_list_urls = load_json(voc_list_urls)
        return json_list_urls

    def _save_data(self, data_lang:dict, data: list, use_s3: bool) -> None:
        """Save the data on S3 or locally

        Args:
            data_lang (dict): data related to the language
            data (list): List of urls of vocabulary lists to scrap
            use_s3 (bool): True if we save the data on s3 else False
        """
        if not use_s3:
            save_data(
                SCRAP_URLS_PATH_FILE.format(data_lang.get("language")),
                json.dumps(data),
            )
        else:
            save_data_on_s3(
                SCRAP_URL_FILE_NAME.format(data_lang.get("language")), data
            )

    def run(self, mongo_hook, lang:str) -> list:
        """
            Run the component to extract the urls of the vocabulary lists from the global url
        Args:
            data_lang (str): language of the data to scrap

        Returns:
            list: list of urls to scrap
        """
        data_lang = find_data(mongo_hook, os.environ["MONGO_DB_DEV"], "constants", {"language": lang}, True)
        if not data_lang:
            return []

        # Get data if not saved
        response = self.get_data(data_lang.get("crawling_url"))
        if not response:
            return voc_list_urls

        voc_list_urls = self._extract_urls_in_bs4(response)
        
        # Format the data for mongo
        voc_list_urls = [{"scrap_url": voc_list_url["scrap_url"], 'language':lang, "status":"WAITING"} for voc_list_url in voc_list_urls]
        
        # Remove the data and add the new data
        remove_data(mongo_hook, os.environ["MONGO_DB_DEV"], "parameters", {"language": lang})
        add_data(mongo_hook, os.environ["MONGO_DB_DEV"], "parameters", voc_list_urls)
