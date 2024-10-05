from urllib.parse import urlparse
from bs4 import BeautifulSoup
import requests

from utils.s3_utils import save_data_on_s3


def get_category(scrap_url: str) -> str:
    name = urlparse(scrap_url).path.split("/")[-1]

    # Remove -pdf from the name
    name = name.replace("-pdf", "")

    # Replace - with _
    name = name.replace("-", "_")

    return name


def get_vocabulary(scrap_url: str) -> list:
    response = requests.get(scrap_url)
    soup = BeautifulSoup(response.text, "html.parser")
    vocabulary_list = []

    # Get title of the page
    category = get_category(scrap_url)

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
