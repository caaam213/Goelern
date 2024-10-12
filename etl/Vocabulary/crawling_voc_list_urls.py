import json
import os
from typing import Union
from bs4 import BeautifulSoup
import requests

from utils.json_utils import load_json
from utils.local_files_utils import load_data, save_data
from utils.s3_utils import get_data_from_s3, save_data_on_s3


class CrawlingVocListUrls:
    """
    This class is responsible for extracting the urls of the vocabulary lists from the global url
    """

    def __init__(self, global_url: str):
        self.global_url = global_url
        self.local_path = (
            f'{os.environ["DATA_PATH"]}/{os.environ["SCRAP_URLS_PATH_FILE"]}'
        )

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

    def _get_data_from_saved_file(self, use_s3: bool) -> Union[list, None]:
        """Get the data from the saved file

        Args:
            use_s3 (bool): True if we get the data from s3 else False

        Returns:
            Union[list, None]: List of urls of vocabulary lists to scrap or None if not found
        """

        if not use_s3:
            voc_list_urls = load_data(self.local_path)
        else:
            voc_list_urls = get_data_from_s3(os.environ["SCRAP_URLS_PATH_S3"])

            if voc_list_urls:
                voc_list_urls = voc_list_urls.decode("utf-8").replace("'", '"')

        json_list_urls = load_json(voc_list_urls)
        return json_list_urls

    def _save_data(self, data: list, use_s3: bool) -> None:
        """Save the data on S3 or locally

        Args:
            data (list): List of urls of vocabulary lists to scrap
            use_s3 (bool): True if we save the data on s3 else False
        """
        if not use_s3:
            save_data(self.local_path, json.dumps(data))
        else:
            save_data_on_s3(os.environ["SCRAP_URLS_PATH_FILE"], data)

    def run(self, use_s3: bool = False) -> list:

        # Check if data is already saved
        voc_list_urls = self._get_data_from_saved_file(use_s3)
        if voc_list_urls:
            return voc_list_urls

        # If data is not found, scrap the global url
        response = requests.get(self.global_url)
        
        if response.status_code != 200:
            print(f"Error: {response.status_code}")
            return []
        
        voc_list_urls = self._extract_urls_in_bs4(response)

        # Save the data on S3 or locally
        self._save_data(voc_list_urls, use_s3)

        return voc_list_urls
