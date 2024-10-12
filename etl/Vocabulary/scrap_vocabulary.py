from urllib.parse import urlparse
from bs4 import BeautifulSoup
import requests


class ScrapVocabulary:
    def __init__(self, scrap_url: str):
        self.scrap_url = scrap_url

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

    def _extract_urls_in_bs4(self, response:requests.Response, category:str) -> list:
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

        # First td is the French word, second td is the German word
        for tr_tag in tr_tags:
            td_tags = tr_tag.find_all("td")
            vocabulary_list.append([td_tags[0].text, td_tags[1].text, category])

        return vocabulary_list

    def run(self) -> list:

        # Get title of the page
        category = self._get_category(self.scrap_url)

        response = requests.get(self.scrap_url)

        if response.status_code != 200:
            print(f"Error: {response.status_code}")
            return []

        vocabulary_list = self._extract_urls_in_bs4(response, category)

        return vocabulary_list

