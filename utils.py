"""
Utility Functions
"""

import pandas as pd

#API
from api import fetch_api

from config import API_URLS
#


def read_file(file_path):

    """
    Read CSV or JSON
    """

    if file_path.endswith(".csv"):

        return pd.read_csv(file_path)

    elif file_path.endswith(".json"):

        return pd.read_json(file_path)

    else:

        raise Exception("Unsupported File")


def clean_dataframe(df):

    """
    Replace NaN with None
    """

    return df.where(pd.notnull(df),None)

#py -m pip show snowflake-connector-python

#API code
def save_csv(df,file_path):

    """
    Save dataframe as csv
    """

    df.to_csv(

        file_path,

        index=False

    )