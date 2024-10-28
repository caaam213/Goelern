import re
from urllib.parse import urlparse
from bs4 import BeautifulSoup
import requests

from app.etl.extraction.abstract_request import AbstractRequest



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
        name = urlparse(scrap_url).path.split("/")[-1]

        # Remove -pdf from the name
        name = name.replace("-pdf", "")

        # Replace - with _
        name = name.replace("-", "_")

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
            print("No table found")
            return vocabulary_list

        tr_tags = table_tag.find_all("tr")

        # Remove td which not contains align="LEFT" because they are not words
        tr_tags = [tr_tag for tr_tag in tr_tags if tr_tag.find("td")["align"] == "LEFT"]

        # First td is the French word, second td is the word in the targeted language
        for tr_tag in tr_tags:
            td_tags = tr_tag.find_all("td")
            vocabulary_list.append([td_tags[0].text, td_tags[1].text, category])

        return vocabulary_list

    def verify_scrap_url(self, scrap_url: str) -> bool:
        """Verify if the url is valid using regex

        Args:
            scrap_url (str): url to verify

        Returns:
            bool: True if the url is valid else False
        """
        return bool(re.match(self.pattern, scrap_url))

    def run(self, scrap_url: str) -> list:
        """Run the component

        Args:
            scrap_url (str): url to scrap

        Returns:
            list: list of words and their translations
        """

        # Verify if url is valid
        if not self.verify_scrap_url(scrap_url):
            print("Scrap url is not valid")
            return []

        # Get title of the page
        category = self._get_category(scrap_url)

        response = self.get_data(scrap_url)

        if not response:
            return []

        vocabulary_list = self._extract_urls_in_bs4(response, category)

        return vocabulary_list
