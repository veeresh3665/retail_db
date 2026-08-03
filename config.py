"""
Read all configurations
"""

from dotenv import load_dotenv
import os

load_dotenv()

# Snowflake Configuration


DATABASE = os.getenv("SNOWFLAKE_DATABASE")
SCHEMA = os.getenv("SNOWFLAKE_SCHEMA")

# Snowflake Configuration
SNOWFLAKE_CONFIG = {
    "user": os.getenv("SNOWFLAKE_USER"),
    "password": os.getenv("SNOWFLAKE_PASSWORD"),
    "account": os.getenv("SNOWFLAKE_ACCOUNT"),
    "warehouse": os.getenv("SNOWFLAKE_WAREHOUSE"),
    "database": DATABASE,
    "schema": SCHEMA
}

# Root Folder

DATA_FOLDER=os.getenv("DATA_FOLDER")

#API
API_URLS={

"USERS":os.getenv("USERS_API"),

"PRODUCTS":os.getenv("PRODUCTS_API"),

"CARTS":os.getenv("CARTS_API")

}