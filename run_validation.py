from db import get_connection
from checks import *
from datetime import datetime


conn=get_connection()

cursor=conn.cursor()



def insert_result(
        table,
        check,
        result,
        failed_records):


    query="""

    INSERT INTO RETAIL_DB.AUDIT.DATA_QUALITY_RESULTS
    (
    TABLE_NAME,
    CHECK_NAME,
    RESULT,
    FAILED_RECORDS,
    RUN_DATE
    )

    VALUES
    (
    %s,%s,%s,%s,%s
    )

    """


    cursor.execute(
        query,
        (
        table,
        check,
        result,
        failed_records,
        datetime.now()
        )
    )



# CHECK 1

table="RETAIL_DB1..DIM_CUSTOMERS"

result,count=null_check(
    cursor,
    table,
    "CUSTOMER_ID"
)


insert_result(
    table,
    "NULL CUSTOMER_ID CHECK",
    result,
    count
)



# CHECK 2

table="RETAIL_DB.ANALYTICS.FACT_ORDERS"


result,count=duplicate_check(
    cursor,
    table,
    "ORDER_ID"
)


insert_result(
    table,
    "DUPLICATE ORDER_ID CHECK",
    result,
    count
)



conn.commit()


print("Validation Completed")