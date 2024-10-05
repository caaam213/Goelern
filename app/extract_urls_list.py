import os
from bs4 import BeautifulSoup

from utils.json_utils import load_json
from utils.s3_utils import get_data_from_s3, save_data_on_s3


class ExtractAndStoreUrlsList:
    """
    This class is responsible for extracting the urls of the vocabulary lists from the global url
    """

    def __init__(self, global_url: str):
        self.global_url = global_url

    def extract_urls_in_bs4(self, html_page) -> list:
        voc_list_urls = []

        soup = BeautifulSoup(html_page.text, "html.parser")
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

    # TODO : Create another method to save data locally and not on S3 ? Maybe not necessary if we add a parameter to the method
    def extract_urls(self) -> list:
        """
        This method extracts the urls of the vocabulary lists from the global url

        Returns:
            list: List of urls of the vocabulary lists
        """

        voc_list_urls = get_data_from_s3(os.environ["SCRAP_URLS_PATH_S3"])
        if voc_list_urls:
            json_list_urls = load_json(voc_list_urls.decode("utf-8").replace("'", '"'))

            if json_list_urls:
                print(json_list_urls)
                return voc_list_urls 

        # If data is not found in S3, scrap the global url
        # response = requests.get(self.global_url)
        # voc_list_urls = self.extract_urls_in_bs4(response)

        # # Save the data on S3
        # save_data_on_s3(os.environ["SCRAP_URLS_PATH_S3"], voc_list_urls)

        return voc_list_urls

    def process(self):
        self.extract_urls()


ExtractAndStoreUrlsList(
    "https://fichesvocabulaire.com/vocabulaire-allemand-pdf"
).process()
