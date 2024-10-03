import requests
from bs4 import BeautifulSoup

def get_voc_list_urls(global_url:str)->list:
    response = requests.get(global_url)
    soup = BeautifulSoup(response.text, 'html.parser')
    voc_list_urls = []

    # Get ol tag
    ol_tag = soup.find('ol')
    
    if ol_tag is None:
        print("No li tags found")
        return voc_list_urls
    
    # Get all li tags
    li_tags = ol_tag.find_all('li')
    
    # Get links in li tags
    for li_tag in li_tags:
        a_tag = li_tag.find('a')
        voc_list_urls.append(a_tag['href'])
    
    return voc_list_urls


get_voc_list_urls("https://fichesvocabulaire.com/vocabulaire-allemand-pdf")
