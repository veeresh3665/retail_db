from datetime import datetime, timedelta

from airflow import DAG
from airflow.providers.standard.operators.empty import EmptyOperator
from airflow.providers.standard.operators.python import PythonOperator
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook
from airflow.utils.task_group import TaskGroup
from airflow.utils.email import send_email


# ============================================================
# CONFIGURATION
# ============================================================

SNOWFLAKE_CONN_ID = "snowflake_retail"

ALERT_EMAIL = "chakaliveeresh137@gmail.com"

DATABASE = "RETAIL_DB"

RAW_SCHEMA = "RAW"
STAGING_SCHEMA = "V_STG_STAGING"
DIM_SCHEMA = "V_STG_DIM"
FACT_SCHEMA = "V_STG_FACT"
MART_SCHEMA = "V_STG_MART"


# ============================================================
# RAW -> STAGING MAPPINGS
# ============================================================
#
# Change table names here ONLY if your actual Snowflake
# table names are different.
#
# ============================================================

RAW_TO_STAGING = [

    (
        f"{DATABASE}.{RAW_SCHEMA}.CUSTOMERS",
        f"{DATABASE}.{STAGING_SCHEMA}.STG_CUSTOMERS",
        "customers",
    ),

    (
        f"{DATABASE}.{RAW_SCHEMA}.PRODUCTS",
        f"{DATABASE}.{STAGING_SCHEMA}.STG_PRODUCTS",
        "products",
    ),

    (
        f"{DATABASE}.{RAW_SCHEMA}.CATEGORIES",
        f"{DATABASE}.{STAGING_SCHEMA}.STG_CATEGORIES",
        "categories",
    ),

    (
        f"{DATABASE}.{RAW_SCHEMA}.SUPPLIERS",
        f"{DATABASE}.{STAGING_SCHEMA}.STG_SUPPLIERS",
        "suppliers",
    ),

    (
        f"{DATABASE}.{RAW_SCHEMA}.STORES",
        f"{DATABASE}.{STAGING_SCHEMA}.STG_STORES",
        "stores",
    ),

    (
        f"{DATABASE}.{RAW_SCHEMA}.EMPLOYEES",
        f"{DATABASE}.{STAGING_SCHEMA}.STG_EMPLOYEES",
        "employees",
    ),

    (
        f"{DATABASE}.{RAW_SCHEMA}.ORDERS",
        f"{DATABASE}.{STAGING_SCHEMA}.STG_ORDERS",
        "orders",
    ),

    (
        f"{DATABASE}.{RAW_SCHEMA}.ORDER_ITEMS",
        f"{DATABASE}.{STAGING_SCHEMA}.STG_ORDER_ITEMS",
        "order_items",
    ),

    (
        f"{DATABASE}.{RAW_SCHEMA}.PAYMENTS",
        f"{DATABASE}.{STAGING_SCHEMA}.STG_PAYMENTS",
        "payments",
    ),

    (
        f"{DATABASE}.{RAW_SCHEMA}.SHIPMENTS",
        f"{DATABASE}.{STAGING_SCHEMA}.STG_SHIPMENTS",
        "shipments",
    ),

    (
        f"{DATABASE}.{RAW_SCHEMA}.INVENTORY",
        f"{DATABASE}.{STAGING_SCHEMA}.STG_INVENTORY",
        "inventory",
    ),

    (
        f"{DATABASE}.{RAW_SCHEMA}.REVIEWS",
        f"{DATABASE}.{STAGING_SCHEMA}.STG_REVIEWS",
        "reviews",
    ),

]


# ============================================================
# STAGING -> DIMENSION MAPPINGS
# ============================================================

STAGING_TO_DIM = [

    (
        f"{DATABASE}.{STAGING_SCHEMA}.STG_CUSTOMERS",
        f"{DATABASE}.{DIM_SCHEMA}.DIM_CUSTOMER",
        "customer_dimension",
    ),

    (
        f"{DATABASE}.{STAGING_SCHEMA}.STG_PRODUCTS",
        f"{DATABASE}.{DIM_SCHEMA}.DIM_PRODUCT",
        "product_dimension",
    ),

    (
        f"{DATABASE}.{STAGING_SCHEMA}.STG_STORES",
        f"{DATABASE}.{DIM_SCHEMA}.DIM_STORE",
        "store_dimension",
    ),

]


# ============================================================
# FACT TABLES
# ============================================================

FACT_TABLES = [

    (
        f"{DATABASE}.{FACT_SCHEMA}.FACT_SALES",
        "fact_sales",
    ),

    (
        f"{DATABASE}.{FACT_SCHEMA}.FACT_INVENTORY",
        "fact_inventory",
    ),

]


# ============================================================
# MART TABLES
# ============================================================

MART_TABLES = [

    (
        f"{DATABASE}.{MART_SCHEMA}.MART_INVENTORY_SUMMARY",
        "mart_inventory_summary",
    ),

]


# ============================================================
# SNOWFLAKE HOOK
# ============================================================

def get_snowflake_hook():

    return SnowflakeHook(
        snowflake_conn_id=SNOWFLAKE_CONN_ID
    )


# ============================================================
# RAW -> STAGING ROW COUNT CHECK
# ============================================================

def compare_row_counts(
    source_table,
    target_table,
    check_name,
):

    hook = get_snowflake_hook()

    source_sql = f"""
        SELECT COUNT(*)
        FROM {source_table}
    """

    target_sql = f"""
        SELECT COUNT(*)
        FROM {target_table}
    """

    print("=" * 70)

    print(f"RECONCILIATION CHECK: {check_name}")

    print(f"Source table : {source_table}")
    print(f"Target table : {target_table}")

    source_result = hook.get_first(source_sql)
    target_result = hook.get_first(target_sql)

    source_count = source_result[0]
    target_count = target_result[0]

    print(f"Source count : {source_count}")
    print(f"Target count : {target_count}")

    print("=" * 70)

    if source_count != target_count:

        raise ValueError(
            f"ROW COUNT MISMATCH | "
            f"CHECK={check_name} | "
            f"SOURCE={source_table} | "
            f"SOURCE_COUNT={source_count} | "
            f"TARGET={target_table} | "
            f"TARGET_COUNT={target_count}"
        )

    print(
        f"PASS: {check_name} | "
        f"Rows={source_count}"
    )


# ============================================================
# RAW -> STAGING INDIVIDUAL CHECKS
# ============================================================

def check_customers():

    compare_row_counts(
        RAW_TO_STAGING[0][0],
        RAW_TO_STAGING[0][1],
        RAW_TO_STAGING[0][2],
    )


def check_products():

    compare_row_counts(
        RAW_TO_STAGING[1][0],
        RAW_TO_STAGING[1][1],
        RAW_TO_STAGING[1][2],
    )


def check_categories():

    compare_row_counts(
        RAW_TO_STAGING[2][0],
        RAW_TO_STAGING[2][1],
        RAW_TO_STAGING[2][2],
    )


def check_suppliers():

    compare_row_counts(
        RAW_TO_STAGING[3][0],
        RAW_TO_STAGING[3][1],
        RAW_TO_STAGING[3][2],
    )


def check_stores():

    compare_row_counts(
        RAW_TO_STAGING[4][0],
        RAW_TO_STAGING[4][1],
        RAW_TO_STAGING[4][2],
    )


def check_employees():

    compare_row_counts(
        RAW_TO_STAGING[5][0],
        RAW_TO_STAGING[5][1],
        RAW_TO_STAGING[5][2],
    )


def check_orders():

    compare_row_counts(
        RAW_TO_STAGING[6][0],
        RAW_TO_STAGING[6][1],
        RAW_TO_STAGING[6][2],
    )


def check_order_items():

    compare_row_counts(
        RAW_TO_STAGING[7][0],
        RAW_TO_STAGING[7][1],
        RAW_TO_STAGING[7][2],
    )


def check_payments():

    compare_row_counts(
        RAW_TO_STAGING[8][0],
        RAW_TO_STAGING[8][1],
        RAW_TO_STAGING[8][2],
    )


def check_shipments():

    compare_row_counts(
        RAW_TO_STAGING[9][0],
        RAW_TO_STAGING[9][1],
        RAW_TO_STAGING[9][2],
    )


def check_inventory():

    compare_row_counts(
        RAW_TO_STAGING[10][0],
        RAW_TO_STAGING[10][1],
        RAW_TO_STAGING[10][2],
    )


def check_reviews():

    compare_row_counts(
        RAW_TO_STAGING[11][0],
        RAW_TO_STAGING[11][1],
        RAW_TO_STAGING[11][2],
    )


# ============================================================
# STAGING -> DIMENSION CHECKS
# ============================================================

def check_customer_dimension():

    compare_row_counts(
        STAGING_TO_DIM[0][0],
        STAGING_TO_DIM[0][1],
        STAGING_TO_DIM[0][2],
    )


def check_product_dimension():

    compare_row_counts(
        STAGING_TO_DIM[1][0],
        STAGING_TO_DIM[1][1],
        STAGING_TO_DIM[1][2],
    )


def check_store_dimension():

    compare_row_counts(
        STAGING_TO_DIM[2][0],
        STAGING_TO_DIM[2][1],
        STAGING_TO_DIM[2][2],
    )


# ============================================================
# FACT VALIDATION
# ============================================================

def check_fact_table(
    table_name,
    check_name,
):

    hook = get_snowflake_hook()

    sql = f"""
        SELECT COUNT(*)
        FROM {table_name}
    """

    result = hook.get_first(sql)

    count = result[0]

    print("=" * 70)

    print(f"FACT CHECK: {check_name}")
    print(f"Table: {table_name}")
    print(f"Rows : {count}")

    print("=" * 70)

    if count == 0:

        raise ValueError(
            f"FACT TABLE IS EMPTY | "
            f"TABLE={table_name}"
        )

    print(
        f"PASS: {check_name} | "
        f"Rows={count}"
    )


def check_fact_sales():

    check_fact_table(
        FACT_TABLES[0][0],
        FACT_TABLES[0][1],
    )


def check_fact_inventory():

    check_fact_table(
        FACT_TABLES[1][0],
        FACT_TABLES[1][1],
    )


# ============================================================
# MART VALIDATION
# ============================================================

def check_mart_table(
    table_name,
    check_name,
):

    hook = get_snowflake_hook()

    sql = f"""
        SELECT COUNT(*)
        FROM {table_name}
    """

    result = hook.get_first(sql)

    count = result[0]

    print("=" * 70)

    print(f"MART CHECK: {check_name}")
    print(f"Table: {table_name}")
    print(f"Rows : {count}")

    print("=" * 70)

    if count == 0:

        raise ValueError(
            f"MART TABLE IS EMPTY | "
            f"TABLE={table_name}"
        )

    print(
        f"PASS: {check_name} | "
        f"Rows={count}"
    )


def check_mart_inventory_summary():

    check_mart_table(
        MART_TABLES[0][0],
        MART_TABLES[0][1],
    )


# ============================================================
# SUCCESS EMAIL
# ============================================================

def reconciliation_success_email(context):

    dag_run = context["dag_run"]

    subject = (
        "SUCCESS - Retail Data Reconciliation"
    )

    html_content = f"""
    <html>

    <body>

        <h2>
            Retail Data Reconciliation Successful
        </h2>

        <table
            border="1"
            cellpadding="8"
            cellspacing="0"
        >

            <tr>
                <td><b>DAG</b></td>
                <td>{dag_run.dag_id}</td>
            </tr>

            <tr>
                <td><b>Run ID</b></td>
                <td>{dag_run.run_id}</td>
            </tr>

            <tr>
                <td><b>Status</b></td>
                <td>SUCCESS</td>
            </tr>

        </table>

        <h3>Completed Checks</h3>

        <ul>

            <li>RAW to STAGING row count checks</li>

            <li>STAGING to DIM row count checks</li>

            <li>FACT table validation</li>

            <li>MART table validation</li>

        </ul>

        <p>
            All configured reconciliation checks
            completed successfully.
        </p>

    </body>

    </html>
    """

    send_email(
        to=[ALERT_EMAIL],
        subject=subject,
        html_content=html_content,
    )


# ============================================================
# FAILURE EMAIL
# ============================================================

def reconciliation_failure_email(context):

    task_instance = context["task_instance"]

    exception = context.get("exception")

    if exception:

        error_message = str(exception)

    else:

        error_message = (
            "Unknown reconciliation error."
        )

    subject = (
        "FAILED - Retail Data Reconciliation"
    )

    html_content = f"""
    <html>

    <body>

        <h2>
            Retail Data Reconciliation Failed
        </h2>

        <table
            border="1"
            cellpadding="8"
            cellspacing="0"
        >

            <tr>
                <td><b>DAG</b></td>
                <td>{task_instance.dag_id}</td>
            </tr>

            <tr>
                <td><b>Failed Task</b></td>
                <td>{task_instance.task_id}</td>
            </tr>

            <tr>
                <td><b>Run ID</b></td>
                <td>{task_instance.run_id}</td>
            </tr>

            <tr>
                <td><b>Status</b></td>
                <td>FAILED</td>
            </tr>

        </table>

        <h3>Exact Issue</h3>

        <pre>
{error_message}
        </pre>

        <h3>Possible Causes</h3>

        <ul>

            <li>Fivetran source data problem</li>

            <li>Missing records in RAW</li>

            <li>dbt staging transformation issue</li>

            <li>Filtering or duplicate removal</li>

            <li>Incorrect join condition</li>

            <li>FACT or MART model issue</li>

        </ul>

        <p>
            <b>Airflow Task Log:</b>
            <br>
            <a href="{task_instance.log_url}">
                Open failed task log
            </a>
        </p>

    </body>

    </html>
    """

    send_email(
        to=[ALERT_EMAIL],
        subject=subject,
        html_content=html_content,
    )


# ============================================================
# DEFAULT ARGUMENTS
# ============================================================

default_args = {

    "owner": "Veeresh",

    "depends_on_past": False,

    "retries": 1,

    "retry_delay": timedelta(
        minutes=2
    ),

    "email_on_failure": False,

    "email_on_retry": False,

    "on_failure_callback":
        reconciliation_failure_email,
}


# ============================================================
# DAG DEFINITION
# ============================================================

with DAG(

    dag_id="retail_reconciliation",

    description=(
        "Retail RAW, STAGING, DIM, FACT "
        "and MART reconciliation"
    ),

    default_args=default_args,

    start_date=datetime(
        2026,
        8,
        1,
    ),

    # Manual during initial testing.
    schedule=None,

    catchup=False,

    max_active_runs=1,

    tags=[
        "retail",
        "snowflake",
        "fivetran",
        "dbt",
        "reconciliation",
        "data-quality",
    ],

    on_success_callback=
        reconciliation_success_email,

) as dag:

    # ========================================================
    # START
    # ========================================================

    start = EmptyOperator(
        task_id="start"
    )

    # ========================================================
    # TASK GROUP: RAW -> STAGING (parallel checks)
    # ========================================================
    with TaskGroup(group_id="raw_to_staging"):

        customers = PythonOperator(
            task_id="raw_to_staging_customers",
            python_callable=check_customers,
        )

        products = PythonOperator(
            task_id="raw_to_staging_products",
            python_callable=check_products,
        )

        categories = PythonOperator(
            task_id="raw_to_staging_categories",
            python_callable=check_categories,
        )

        suppliers = PythonOperator(
            task_id="raw_to_staging_suppliers",
            python_callable=check_suppliers,
        )

        stores = PythonOperator(
            task_id="raw_to_staging_stores",
            python_callable=check_stores,
        )

        employees = PythonOperator(
            task_id="raw_to_staging_employees",
            python_callable=check_employees,
        )

        orders = PythonOperator(
            task_id="raw_to_staging_orders",
            python_callable=check_orders,
        )

        order_items = PythonOperator(
            task_id="raw_to_staging_order_items",
            python_callable=check_order_items,
        )

        payments = PythonOperator(
            task_id="raw_to_staging_payments",
            python_callable=check_payments,
        )

        shipments = PythonOperator(
            task_id="raw_to_staging_shipments",
            python_callable=check_shipments,
        )

        inventory = PythonOperator(
            task_id="raw_to_staging_inventory",
            python_callable=check_inventory,
        )

        reviews = PythonOperator(
            task_id="raw_to_staging_reviews",
            python_callable=check_reviews,
        )


    # ========================================================
    # TASK GROUP: STAGING -> DIMENSIONS
    # ========================================================
    with TaskGroup(group_id="staging_to_dimensions"):

        customer_dim = PythonOperator(
            task_id="staging_to_dim_customer",
            python_callable=check_customer_dimension,
        )

        product_dim = PythonOperator(
            task_id="staging_to_dim_product",
            python_callable=check_product_dimension,
        )

        store_dim = PythonOperator(
            task_id="staging_to_dim_store",
            python_callable=check_store_dimension,
        )


    # ========================================================
    # TASK GROUP: FACT VALIDATION
    # ========================================================
    with TaskGroup(group_id="fact_validation"):

        fact_sales = PythonOperator(
            task_id="fact_sales_validation",
            python_callable=check_fact_sales,
        )

        fact_inventory = PythonOperator(
            task_id="fact_inventory_validation",
            python_callable=check_fact_inventory,
        )


    # ========================================================
    # TASK GROUP: MART VALIDATION
    # ========================================================
    with TaskGroup(group_id="mart_validation"):

        mart_inventory = PythonOperator(
            task_id="mart_inventory_validation",
            python_callable=
                check_mart_inventory_summary,
        )


    # ========================================================
    # END
    # ========================================================

    reconciliation_complete = EmptyOperator(
        task_id="reconciliation_complete"
    )

    # ========================================================
    # DEPENDENCIES
    # ========================================================

    # --------------------------------------------------------
    # START -> RAW/STAGING CHECKS
    # All execute in parallel.
    # --------------------------------------------------------

    start >> customers
    start >> products
    start >> categories
    start >> suppliers
    start >> stores
    start >> employees
    start >> orders
    start >> order_items
    start >> payments
    start >> shipments
    start >> inventory
    start >> reviews

    # --------------------------------------------------------
    # STAGING -> DIM
    # --------------------------------------------------------

    customers >> customer_dim
    products >> product_dim
    stores >> store_dim

    # --------------------------------------------------------
    # STAGING -> FACT
    # --------------------------------------------------------

    orders >> fact_sales
    order_items >> fact_sales

    orders >> fact_inventory
    order_items >> fact_inventory
    inventory >> fact_inventory

    # --------------------------------------------------------
    # FACT -> MART
    # --------------------------------------------------------

    fact_inventory >> mart_inventory

    # --------------------------------------------------------
    # ALL CHECKS -> COMPLETE
    # --------------------------------------------------------

    customers >> reconciliation_complete
    products >> reconciliation_complete
    categories >> reconciliation_complete
    suppliers >> reconciliation_complete
    stores >> reconciliation_complete
    employees >> reconciliation_complete
    orders >> reconciliation_complete
    order_items >> reconciliation_complete
    payments >> reconciliation_complete
    shipments >> reconciliation_complete
    inventory >> reconciliation_complete
    reviews >> reconciliation_complete

    customer_dim >> reconciliation_complete
    product_dim >> reconciliation_complete
    store_dim >> reconciliation_complete

    fact_sales >> reconciliation_complete
    fact_inventory >> reconciliation_complete

    mart_inventory >> reconciliation_complete