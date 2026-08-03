from datetime import datetime


def null_check(cursor, table, column):

    query=f"""
    SELECT COUNT(*)
    FROM {table}
    WHERE {column} IS NULL
    """

    cursor.execute(query)

    count=cursor.fetchone()[0]


    if count == 0:
        result="PASS"
    else:
        result="FAIL"


    return result,count



def duplicate_check(cursor, table, column):

    query=f"""
    SELECT COUNT(*)
    FROM
    (
        SELECT {column},
        COUNT(*)
        FROM {table}
        GROUP BY {column}
        HAVING COUNT(*) > 1
    )
    """

    cursor.execute(query)

    count=cursor.fetchone()[0]


    if count == 0:
        result="PASS"
    else:
        result="FAIL"


    return result,count