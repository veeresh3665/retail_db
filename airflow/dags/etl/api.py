"""
API Module
"""

import requests

import pandas as pd


def fetch_api(url):

    """
    Fetch API Data
    """

    response=requests.get(url)

    response.raise_for_status()

    return response.json()