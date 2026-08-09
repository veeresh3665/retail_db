"""
Main ETL Program
"""

import glob
import os
import pandas as pd

from config import DATA_FOLDER
from db import get_connection
from utils import read_file, clean_dataframe


def create_table_if_not_exists(cursor, table, df):
    """
    Create Snowflake table dynamically if it doesn't exist
    """

    columns = []

    for col in df.columns:
        columns.append(f'"{col.upper()}" STRING')

    create_sql = f"""
    CREATE TABLE IF NOT EXISTS {table}
    (
        {",".join(columns)}
    )
    """

    cursor.execute(create_sql)

    print(f"✅ Table {table} is ready")


def load_table(df, table):
    """
    Load dataframe into Snowflake
    """

    # -----------------------------
    # Clean DataFrame
    # -----------------------------
    df = clean_dataframe(df)

    # Replace NaN with None
    df = df.astype(object)
    df = df.where(pd.notna(df), None)

    # Replace blank spaces with NULL
    df = df.replace(r'^\s*$', None, regex=True)

    # -----------------------------
    # Connect to Snowflake
    # -----------------------------
    conn = get_connection()
    cursor = conn.cursor()

    try:

        # --------------------------------
        # Create table automatically
        # --------------------------------
        create_table_if_not_exists(cursor, table, df)

        # --------------------------------
        # Prepare Insert Statement
        # --------------------------------
        columns = ",".join([f'"{col.upper()}"' for col in df.columns])

        placeholders = ",".join(["%s"] * len(df.columns))

        sql = f"""
        INSERT INTO {table}
        ({columns})
        VALUES ({placeholders})
        """

        # Convert dataframe to tuples
        data = list(df.itertuples(index=False, name=None))

        # Bulk Insert
        cursor.executemany(sql, data)

        conn.commit()

        print("=" * 60)
        print(f"✅ {table} Loaded Successfully")
        print(f"✅ Rows Inserted : {len(data)}")
        print("=" * 60)

    except Exception as e:

        conn.rollback()

        print("=" * 60)
        print(f"❌ Failed to load {table}")
        print(e)
        print("=" * 60)

    finally:

        cursor.close()
        conn.close()


def get_table_name(file_name):
    """
    Decide Snowflake table name
    """

    file_name = file_name.lower()

    if "orders1" in file_name:
        return "ORDERS1"

    elif "customers1" in file_name:
        return "CUSTOMERS1"

    elif "products1" in file_name:
        return "PRODUCTS1"

    else:
        return None


def main():
    """
    Read all CSV/JSON files inside DATA_FOLDER
    """

    files = glob.glob(
        os.path.join(DATA_FOLDER, "**", "*"),
        recursive=True
    )

    for file_path in files:

        if not os.path.isfile(file_path):
            continue

        table = get_table_name(os.path.basename(file_path))

        if table is None:
            print(f"Skipping : {file_path}")
            continue

        print("\n" + "=" * 60)
        print(f"Reading File : {file_path}")
        print(f"Target Table : {table}")
        print("=" * 60)

        df = read_file(file_path)

        load_table(df, table)


if __name__ == "__main__":
    main()