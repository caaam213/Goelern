

import sys


sys.path.insert(0, "/opt/")
from airflow.etl.extraction.scrap_vocabulary import ScrapVocabulary
import pytest

@pytest.mark.parametrize("scrap_url, res", [
    ("https://fichesvocabulaire.com/vocabulaire-allemand-pdf", True),
    ("https://fichesvocabulaire.com/listes-vocabulaire-anglais-pdf", True),
    ("https://fichesvocabulaire.com/vocabulaire-espagnol-pdf", True),
    ("https://youtu.be/UX7MGmj_zEA?si=8Q2Rm5YjSq3t06zh", False),
    ("", False)
])
def test_verify_scrap_url(scrap_url, res):
    assert ScrapVocabulary()._verify_scrap_url(scrap_url) == res
    