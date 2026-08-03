import snowflake.connector
from config import SNOWFLAKE_CONFIG

def get_connection():
    try:
        conn = snowflake.connector.connect(**SNOWFLAKE_CONFIG)
        print("✅ Connected Successfully")
        return conn

    except Exception as e:
        print("❌ Connection Failed")
        print(e)
        raise