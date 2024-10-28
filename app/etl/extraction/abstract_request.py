from abc import ABC
from typing import Union

import requests


class AbstractRequest(ABC):

    def get_data(self, url: str) -> Union[requests.Response, None]:
        """Get data using requests library

        Args:
            url (str): url to get data

        Returns:
            Union[requests.Response, None]: response of the request or None
        """
        response = requests.get(url)

        if response.status_code != 200:
            print(f"Error: {response.status_code}")
            return None
        return response
