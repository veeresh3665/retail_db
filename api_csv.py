"""
Fetch data from APIs
Convert JSON to CSV
Store CSV files in DATA_FOLDER
"""

import os
import requests
import pandas as pd

from config import API_URLS, DATA_FOLDER


def fetch_api(url):
    """
    Fetch data from REST API
    """
    response = requests.get(url)
    response.raise_for_status()
    return response.json()


def save_csv(df, folder_name, file_name):
    """
    Save dataframe into a folder as CSV
    """

    folder_path = os.path.join(DATA_FOLDER, folder_name)

    os.makedirs(folder_path, exist_ok=True)

    file_path = os.path.join(folder_path, file_name)

    df.to_csv(file_path, index=False)

    print(f"Saved : {file_path}")


def main():

    # USERS API
    users = fetch_api(API_URLS["USERS"])

    users_df = pd.DataFrame(users["users"])

    save_csv(users_df, "Customers1", "customers1.csv")


    # PRODUCTS API
    products = fetch_api(API_URLS["PRODUCTS"])

    products_df = pd.DataFrame(products["products"])

    save_csv(products_df, "Products1", "products1.csv")


    # CARTS API
    carts = fetch_api(API_URLS["CARTS"])

    carts_df = pd.DataFrame(carts["carts"])

    save_csv(carts_df, "Orders1", "orders1.csv")


if __name__ == "__main__":

    main()