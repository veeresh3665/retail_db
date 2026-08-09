"""
Main ETL Program
Load all CSV/JSON files into Snowflake RAW schema
"""

import glob
import os
import pandas as pd

from config import DATA_FOLDER, DATABASE, SCHEMA
from db import get_connection
from utils import read_file, clean_dataframe


def create_table_if_not_exists(cursor, table, df):
    """
    Create Snowflake table dynamically if it doesn't exist
    """

    full_table_name = f"{DATABASE}.{SCHEMA}.{table}"

    columns = []

    for col in df.columns:
        columns.append(f'"{col.upper()}" STRING')

    create_sql = f"""
    CREATE TABLE IF NOT EXISTS {full_table_name}
    (
        {",".join(columns)}
    )
    """

    cursor.execute(create_sql)

    print(f"✅ Table {full_table_name} is ready")


def load_table(df, table):
    """
    Load dataframe into Snowflake
    """

    # ------------------------------------
    # Clean Data
    # ------------------------------------

    df = clean_dataframe(df)

    df = df.astype(object)
    df = df.where(pd.notna(df), None)

    df = df.replace(r'^\s*$', None, regex=True)

    # ------------------------------------
    # Connect Snowflake
    # ------------------------------------

    conn = get_connection()
    cursor = conn.cursor()

    full_table_name = f"{DATABASE}.{SCHEMA}.{table}"

    try:

        # Create table if not exists
        create_table_if_not_exists(cursor, table, df)

        # Column names
        columns = ",".join(
            [f'"{col.upper()}"' for col in df.columns]
        )

        # Placeholders
        placeholders = ",".join(["%s"] * len(df.columns))

        # Insert Statement
        sql = f"""
        INSERT INTO {full_table_name}
        ({columns})
        VALUES ({placeholders})
        """

        # Convert dataframe into tuples
        data = list(df.itertuples(index=False, name=None))

        # Bulk Insert
        cursor.executemany(sql, data)

        conn.commit()

        print("=" * 60)
        print(f"✅ Loaded Successfully : {full_table_name}")
        print(f"✅ Rows Inserted : {len(data)}")
        print("=" * 60)

    except Exception as e:

        conn.rollback()

        print("=" * 60)
        print(f"❌ Failed Loading : {full_table_name}")
        print(e)
        print("=" * 60)

    finally:

        cursor.close()
        conn.close()


def get_table_name(file_name):
    """
    Convert filename into Snowflake table name

    Example

    customers.csv      -> CUSTOMERS
    order_items.csv    -> ORDER_ITEMS
    """

    table_name = os.path.splitext(file_name)[0]

    return table_name.upper()


def main():
    """
    Read every CSV / JSON file
    """

    files = glob.glob(
        os.path.join(DATA_FOLDER, "**", "*"),
        recursive=True
    )

    for file_path in files:

        if not os.path.isfile(file_path):
            continue

        file_name = os.path.basename(file_path)

        table = get_table_name(file_name)

        print("\n" + "=" * 70)
        print(f"Reading File : {file_path}")
        print(f"Target Table : {DATABASE}.{SCHEMA}.{table}")
        print("=" * 70)

        try:

            df = read_file(file_path)

            if df.empty:
                print(f"⚠ {file_name} is empty. Skipping...")
                continue

            load_table(df, table)

        except Exception as e:

            print(f"❌ Error reading {file_name}")
            print(e)


if __name__ == "__main__":
    main()