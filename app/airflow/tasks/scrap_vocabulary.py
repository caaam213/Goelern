from datetime import datetime
import re
from urllib.parse import urlparse
from bs4 import BeautifulSoup
import requests
import pandas as pd

from utils.utils_mongo.constants_collection import get_data_by_lang
from extraction.abstract_request import AbstractRequest


def init_data_lang(lang: str) -> dict:
    """
    Initialize language data using a given language code.

    Args:
        lang (str): The language code (e.g., 'en' for English).

    Returns:
        dict: A dictionary containing language-specific data mappings.
    """
    return get_data_by_lang(lang)


def get_category(scrap_url: str) -> str:
    """
    Extract the category name from the vocabulary URL.

    Args:
        scrap_url (str): URL of the vocabulary list.

    Returns:
        str: Cleaned category name from the URL.
    """
    name = urlparse(scrap_url).path.split("/")[-1]
    name = name.replace("-pdf", "").replace("-", "_")
    return name


def extract_urls_in_bs4(response: requests.Response, category: str) -> list:
    """
    Extract vocabulary words and their translations from the response HTML.

    Args:
        response (requests.Response): Response from the vocabulary list URL.
        category (str): Category name of the vocabulary list.

    Returns:
        list: List of vocabulary words with their translations in the format
              [[word_in_french, word_in_target_language, category], ...].
    """
    soup = BeautifulSoup(response.text, "html.parser")
    vocabulary_list = []

    table_tag = soup.find("table")
    if not table_tag:
        print("No table found")
        return vocabulary_list

    tr_tags = table_tag.find_all("tr")
    tr_tags = [tr for tr in tr_tags if tr.find("td")["align"] == "LEFT"]

    for tr_tag in tr_tags:
        td_tags = tr_tag.find_all("td")
        vocabulary_list.append([td_tags[0].text, td_tags[1].text, category])

    return vocabulary_list


def verify_scrap_url(scrap_url: str, pattern: str) -> bool:
    """
    Verify if the vocabulary list URL matches the expected pattern.

    Args:
        scrap_url (str): URL to verify.
        pattern (str): Regular expression pattern for URL validation.

    Returns:
        bool: True if the URL is valid, False otherwise.
    """
    return bool(re.match(pattern, scrap_url))


def run_scraping(scrap_url: str, lang: str) -> list:
    """
    Perform the vocabulary scraping process for a given URL and language code.

    Args:
        scrap_url (str): URL of the vocabulary list to scrape.
        lang (str): Language code for retrieving language-specific mappings.

    Returns:
        list: List of words and their translations, saved to a CSV file.
    """
    data_lang = init_data_lang(lang)
    base_name_col = data_lang.get("base_name_col")
    trans_name_col = data_lang.get("translate_name_col")
    pattern = r"^https?:\/\/fichesvocabulaire\.com\/.*"

    if not verify_scrap_url(scrap_url, pattern):
        print("Scrap url is not valid")
        return []

    category = get_category(scrap_url)
    response = AbstractRequest().get_data(scrap_url)

    if not response:
        return []

    vocabulary_list = extract_urls_in_bs4(response, category)
    df_vocabularies = pd.DataFrame(
        vocabulary_list,
        columns=[base_name_col, trans_name_col, "Category"],
    )
    date = datetime.now().strftime("%Y%m%d")
    path_csv_file = f"/opt/airflow/data/metacritic_{date}.csv"
    df_vocabularies.to_csv(path_csv_file, index=False)

    return vocabulary_list
