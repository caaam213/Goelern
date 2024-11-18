from pathlib import Path
from urllib.parse import urlparse


def get_last_path_segment(url: str) -> str:
    """Get the last segment of a URL

    Args:
        url (str): URL to extract the last segment from

    Returns:
        str: Last segment of the URL
    """
    path = urlparse(url).path
    if not path or path == "/":
        return ""
    return Path(path).name
