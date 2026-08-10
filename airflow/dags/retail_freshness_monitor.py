from datetime import datetime, timedelta

from airflow import DAG
from airflow.providers.standard.operators.empty import EmptyOperator
from airflow.providers.standard.operators.python import PythonOperator
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook
from airflow.utils.email import send_email


# ============================================================
# CONFIGURATION
# ============================================================

SNOWFLAKE_CONN_ID = "snowflake_retail"

ALERT_EMAIL = "chakaliveeresh137@gmail.com"

# Maximum allowed age of Fivetran data
# Change this according to your Fivetran schedule.
FRESHNESS_THRESHOLD_MINUTES = 600


# ============================================================
# GENERIC FRESHNESS CHECK
# ============================================================

def check_table_freshness(table_name):

    hook = SnowflakeHook(
        snowflake_conn_id=SNOWFLAKE_CONN_ID
    )

    sql = f"""
        SELECT
            MAX(_FIVETRAN_SYNCED) AS LAST_SYNCED
        FROM {table_name}
    """

    result = hook.get_first(sql)

    if not result:
        raise ValueError(
            f"{table_name}: Query returned no result."
        )

    last_synced = result[0]

    if last_synced is None:
        raise ValueError(
            f"{table_name}: No Fivetran sync timestamp found."
        )

    # Get current Snowflake time
    current_time_sql = """
        SELECT CURRENT_TIMESTAMP()
    """

    current_result = hook.get_first(
        current_time_sql
    )

    current_time = current_result[0]

    age = current_time - last_synced

    age_minutes = age.total_seconds() / 60

    print("=" * 70)

    print(
        f"FIVETRAN FRESHNESS CHECK"
    )

    print(
        f"Table       : {table_name}"
    )

    print(
        f"Last Synced : {last_synced}"
    )

    print(
        f"Current Time: {current_time}"
    )

    print(
        f"Data Age    : {age_minutes:.2f} minutes"
    )

    print(
        f"Threshold   : "
        f"{FRESHNESS_THRESHOLD_MINUTES} minutes"
    )

    print("=" * 70)

    if age_minutes > FRESHNESS_THRESHOLD_MINUTES:

        raise ValueError(
            f"STALE DATA: {table_name}. "
            f"Last Fivetran sync was "
            f"{age_minutes:.2f} minutes ago. "
            f"Threshold is "
            f"{FRESHNESS_THRESHOLD_MINUTES} minutes."
        )

    print(
        f"FRESH DATA: {table_name}"
    )


# ============================================================
# INDIVIDUAL TABLE CHECKS
# ============================================================

def check_customers_freshness():

    check_table_freshness(
        "RETAIL_DB.RAW.CUSTOMERS"
    )


def check_products_freshness():

    check_table_freshness(
        "RETAIL_DB.RAW.PRODUCTS"
    )


def check_categories_freshness():

    check_table_freshness(
        "RETAIL_DB.RAW.CATEGORIES"
    )


def check_suppliers_freshness():

    check_table_freshness(
        "RETAIL_DB.RAW.SUPPLIERS"
    )


def check_stores_freshness():

    check_table_freshness(
        "RETAIL_DB.RAW.STORES"
    )


def check_employees_freshness():

    check_table_freshness(
        "RETAIL_DB.RAW.EMPLOYEES"
    )


def check_orders_freshness():

    check_table_freshness(
        "RETAIL_DB.RAW.ORDERS"
    )


def check_order_items_freshness():

    check_table_freshness(
        "RETAIL_DB.RAW.ORDER_ITEMS"
    )


def check_payments_freshness():

    check_table_freshness(
        "RETAIL_DB.RAW.PAYMENTS"
    )


def check_shipments_freshness():

    check_table_freshness(
        "RETAIL_DB.RAW.SHIPMENTS"
    )


def check_inventory_freshness():

    check_table_freshness(
        "RETAIL_DB.RAW.INVENTORY"
    )


def check_reviews_freshness():

    check_table_freshness(
        "RETAIL_DB.RAW.REVIEWS"
    )


# ============================================================
# SUCCESS EMAIL
# ============================================================

def freshness_success_email(context):

    dag_run = context["dag_run"]

    dag_id = dag_run.dag_id

    run_id = dag_run.run_id

    subject = (
        f"✅ FIVETRAN FRESHNESS PASSED | {dag_id}"
    )

    html_content = f"""
    <html>

    <body>

        <h2>
            ✅ Fivetran Data Freshness Check Passed
        </h2>

        <p>
            All configured Fivetran RAW tables
            contain data within the allowed
            freshness threshold.
        </p>

        <table
            border="1"
            cellpadding="8"
            cellspacing="0"
        >

            <tr>
                <td><b>DAG</b></td>
                <td>{dag_id}</td>
            </tr>

            <tr>
                <td><b>Run ID</b></td>
                <td>{run_id}</td>
            </tr>

            <tr>
                <td><b>Status</b></td>
                <td>SUCCESS</td>
            </tr>

            <tr>
                <td><b>Threshold</b></td>
                <td>
                    {FRESHNESS_THRESHOLD_MINUTES} minutes
                </td>
            </tr>

        </table>

        <h3>Tables Checked</h3>

        <ul>

            <li>Customers</li>
            <li>Products</li>
            <li>Categories</li>
            <li>Suppliers</li>
            <li>Stores</li>
            <li>Employees</li>
            <li>Orders</li>
            <li>Order Items</li>
            <li>Payments</li>
            <li>Shipments</li>
            <li>Inventory</li>
            <li>Reviews</li>

        </ul>

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

def freshness_failure_email(context):

    task_instance = context["task_instance"]

    dag_id = task_instance.dag_id

    task_id = task_instance.task_id

    run_id = task_instance.run_id

    exception = context.get(
        "exception"
    )

    if exception:

        error_message = str(exception)

    else:

        error_message = (
            "Unknown freshness monitoring failure."
        )

    log_url = task_instance.log_url

    subject = (
        f"❌ FIVETRAN FRESHNESS FAILED | "
        f"{dag_id} | {task_id}"
    )

    html_content = f"""
    <html>

    <body>

        <h2>
            ❌ Fivetran Data Freshness Alert
        </h2>

        <table
            border="1"
            cellpadding="8"
            cellspacing="0"
        >

            <tr>
                <td><b>DAG</b></td>
                <td>{dag_id}</td>
            </tr>

            <tr>
                <td><b>Failed Task</b></td>
                <td>{task_id}</td>
            </tr>

            <tr>
                <td><b>Run ID</b></td>
                <td>{run_id}</td>
            </tr>

            <tr>
                <td><b>Status</b></td>
                <td>FAILED</td>
            </tr>

            <tr>
                <td><b>Threshold</b></td>
                <td>
                    {FRESHNESS_THRESHOLD_MINUTES}
                    minutes
                </td>
            </tr>

        </table>

        <h3>Issue</h3>

        <pre>
{error_message}
        </pre>

        <p>
            <b>Airflow Log:</b>
            <br>

            <a href="{log_url}">
                Open failed task log
            </a>

        </p>

        <h3>Possible Causes</h3>

        <ul>

            <li>Fivetran sync has failed.</li>

            <li>Fivetran sync is delayed.</li>

            <li>Source SharePoint data has not changed.</li>

            <li>Fivetran connector is paused.</li>

            <li>Network/connectivity problem.</li>

            <li>Freshness threshold is too small.</li>

        </ul>

    </body>

    </html>
    """

    send_email(
        to=[ALERT_EMAIL],
        subject=subject,
        html_content=html_content,
    )


# ============================================================
# DEFAULT ARGS
# ============================================================

default_args = {

    "owner": "Veeresh",

    "depends_on_past": False,

    "retries": 1,

    "retry_delay": timedelta(
        minutes=2
    ),

    # We use our own detailed callback.
    "email_on_failure": False,

    "email_on_retry": False,

    "on_failure_callback":
        freshness_failure_email,
}


# ============================================================
# DAG
# ============================================================

with DAG(

    dag_id="retail_freshness_monitor",

    description=(
        "Monitor Fivetran data freshness "
        "in Snowflake RAW tables"
    ),

    default_args=default_args,

    start_date=datetime(
        2026,
        8,
        1
    ),

    # Run manually for initial testing.
    # Later we will change this to:
    # schedule="*/30 * * * *"
    #
    # meaning every 30 minutes.
    schedule=None,

    catchup=False,

    tags=[
        "retail",
        "fivetran",
        "freshness",
        "snowflake",
        "monitoring",
    ],

    on_success_callback=
        freshness_success_email,

) as dag:

    # ========================================================
    # START
    # ========================================================

    start = EmptyOperator(
        task_id="start"
    )

    # ========================================================
    # PARALLEL FRESHNESS CHECKS
    # ========================================================

    customers_freshness = PythonOperator(

        task_id="customers_freshness",

        python_callable=
            check_customers_freshness,
    )

    products_freshness = PythonOperator(

        task_id="products_freshness",

        python_callable=
            check_products_freshness,
    )

    categories_freshness = PythonOperator(

        task_id="categories_freshness",

        python_callable=
            check_categories_freshness,
    )

    suppliers_freshness = PythonOperator(

        task_id="suppliers_freshness",

        python_callable=
            check_suppliers_freshness,
    )

    stores_freshness = PythonOperator(

        task_id="stores_freshness",

        python_callable=
            check_stores_freshness,
    )

    employees_freshness = PythonOperator(

        task_id="employees_freshness",

        python_callable=
            check_employees_freshness,
    )

    orders_freshness = PythonOperator(

        task_id="orders_freshness",

        python_callable=
            check_orders_freshness,
    )

    order_items_freshness = PythonOperator(

        task_id="order_items_freshness",

        python_callable=
            check_order_items_freshness,
    )

    payments_freshness = PythonOperator(

        task_id="payments_freshness",

        python_callable=
            check_payments_freshness,
    )

    shipments_freshness = PythonOperator(

        task_id="shipments_freshness",

        python_callable=
            check_shipments_freshness,
    )

    inventory_freshness = PythonOperator(

        task_id="inventory_freshness",

        python_callable=
            check_inventory_freshness,
    )

    reviews_freshness = PythonOperator(

        task_id="reviews_freshness",

        python_callable=
            check_reviews_freshness,
    )

    # ========================================================
    # COMPLETE
    # ========================================================

    freshness_complete = EmptyOperator(

        task_id="freshness_complete"
    )

    # ========================================================
    # PARALLEL DEPENDENCIES
    # ========================================================

    start >> [

        customers_freshness,

        products_freshness,

        categories_freshness,

        suppliers_freshness,

        stores_freshness,

        employees_freshness,

        orders_freshness,

        order_items_freshness,

        payments_freshness,

        shipments_freshness,

        inventory_freshness,

        reviews_freshness,

    ] >> freshness_complete