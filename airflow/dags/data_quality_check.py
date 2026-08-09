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


# ============================================================
# GENERIC DATA QUALITY FUNCTION
# ============================================================

def run_quality_check(table_name, check_name, sql):
    """
    Execute one Snowflake data-quality check.

    The query must return a single numeric value.

    0  = PASS
    >0 = FAIL
    """

    hook = SnowflakeHook(
        snowflake_conn_id=SNOWFLAKE_CONN_ID
    )

    print("=" * 70)
    print(f"DATA QUALITY CHECK: {check_name}")
    print(f"TABLE: {table_name}")
    print("=" * 70)

    result = hook.get_first(sql)

    if not result:
        raise ValueError(
            f"{check_name}: Query returned no result."
        )

    violation_count = result[0]

    print(
        f"{check_name} violation count: "
        f"{violation_count}"
    )

    if violation_count is None:
        raise ValueError(
            f"{check_name}: Result was NULL."
        )

    if violation_count > 0:

        raise ValueError(
            f"DATA QUALITY FAILED: "
            f"{check_name}. "
            f"Found {violation_count} violation(s) "
            f"in {table_name}."
        )

    print(
        f"DATA QUALITY PASSED: {check_name}"
    )


# ============================================================
# CUSTOM CHECK FUNCTIONS
# ============================================================

def check_customers():

    run_quality_check(

        table_name="RETAIL_DB.RAW.CUSTOMERS",

        check_name="CUSTOMERS_NULL_CUSTOMER_ID",

        sql="""
            SELECT COUNT(*)
            FROM RETAIL_DB.RAW.CUSTOMERS
            WHERE CUSTOMER_ID IS NULL
        """
    )


def check_products():

    run_quality_check(

        table_name="RETAIL_DB.RAW.PRODUCTS",

        check_name="PRODUCTS_NULL_PRODUCT_ID",

        sql="""
            SELECT COUNT(*)
            FROM RETAIL_DB.RAW.PRODUCTS
            WHERE PRODUCT_ID IS NULL
        """
    )


def check_categories():

    run_quality_check(

        table_name="RETAIL_DB.RAW.CATEGORIES",

        check_name="CATEGORIES_NULL_CATEGORY_ID",

        sql="""
            SELECT COUNT(*)
            FROM RETAIL_DB.RAW.CATEGORIES
            WHERE CATEGORY_ID IS NULL
        """
    )


def check_suppliers():

    run_quality_check(

        table_name="RETAIL_DB.RAW.SUPPLIERS",

        check_name="SUPPLIERS_NULL_SUPPLIER_ID",

        sql="""
            SELECT COUNT(*)
            FROM RETAIL_DB.RAW.SUPPLIERS
            WHERE SUPPLIER_ID IS NULL
        """
    )


def check_stores():

    run_quality_check(

        table_name="RETAIL_DB.RAW.STORES",

        check_name="STORES_NULL_STORE_ID",

        sql="""
            SELECT COUNT(*)
            FROM RETAIL_DB.RAW.STORES
            WHERE STORE_ID IS NULL
        """
    )


def check_employees():

    run_quality_check(

        table_name="RETAIL_DB.RAW.EMPLOYEES",

        check_name="EMPLOYEES_NULL_EMPLOYEE_ID",

        sql="""
            SELECT COUNT(*)
            FROM RETAIL_DB.RAW.EMPLOYEES
            WHERE EMPLOYEE_ID IS NULL
        """
    )


def check_orders():

    run_quality_check(

        table_name="RETAIL_DB.RAW.ORDERS",

        check_name="ORDERS_NULL_ORDER_ID",

        sql="""
            SELECT COUNT(*)
            FROM RETAIL_DB.RAW.ORDERS
            WHERE ORDER_ID IS NULL
        """
    )


def check_order_items():

    run_quality_check(

        table_name="RETAIL_DB.RAW.ORDER_ITEMS",

        check_name="ORDER_ITEMS_NULL_ORDER_ITEM_ID",

        sql="""
            SELECT COUNT(*)
            FROM RETAIL_DB.RAW.ORDER_ITEMS
            WHERE ORDER_ITEM_ID IS NULL
        """
    )


def check_payments():

    run_quality_check(

        table_name="RETAIL_DB.RAW.PAYMENTS",

        check_name="PAYMENTS_NULL_PAYMENT_ID",

        sql="""
            SELECT COUNT(*)
            FROM RETAIL_DB.RAW.PAYMENTS
            WHERE PAYMENT_ID IS NULL
        """
    )


def check_shipments():

    run_quality_check(

        table_name="RETAIL_DB.RAW.SHIPMENTS",

        check_name="SHIPMENTS_NULL_SHIPMENT_ID",

        sql="""
            SELECT COUNT(*)
            FROM RETAIL_DB.RAW.SHIPMENTS
            WHERE SHIPMENT_ID IS NULL
        """
    )


def check_inventory():

    run_quality_check(

        table_name="RETAIL_DB.RAW.INVENTORY",

        check_name="INVENTORY_NEGATIVE_STOCK",

        sql="""
            SELECT COUNT(*)
            FROM RETAIL_DB.RAW.INVENTORY
            WHERE STOCK < 0
        """
    )


def check_reviews():

    run_quality_check(

        table_name="RETAIL_DB.RAW.REVIEWS",

        check_name="REVIEWS_NULL_REVIEW_ID",

        sql="""
            SELECT COUNT(*)
            FROM RETAIL_DB.RAW.REVIEWS
            WHERE REVIEW_ID IS NULL
        """
    )


# ============================================================
# SUCCESS EMAIL
# ============================================================

def quality_success_email(context):

    dag_run = context["dag_run"]

    dag_id = dag_run.dag_id

    run_id = dag_run.run_id

    subject = (
        f"✅ DATA QUALITY PASSED | {dag_id}"
    )

    html_content = f"""

    <html>

    <body>

        <h2>
            ✅ Retail Data Quality Passed
        </h2>

        <p>
            All configured Snowflake RAW
            data-quality checks completed
            successfully.
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

        </table>

        <h3>Checks Completed</h3>

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

def quality_failure_email(context):

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
            "Unknown data quality failure."
        )

    log_url = task_instance.log_url

    subject = (
        f"❌ DATA QUALITY FAILED | "
        f"{dag_id} | {task_id}"
    )

    html_content = f"""

    <html>

    <body>

        <h2>
            ❌ Retail Data Quality Failed
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

    "email_on_failure": False,

    "email_on_retry": False,

    "on_failure_callback":
        quality_failure_email,
}


# ============================================================
# DAG
# ============================================================

with DAG(

    dag_id="retail_data_quality",

    description=(
        "Parallel Snowflake RAW "
        "data quality validation"
    ),

    default_args=default_args,

    start_date=datetime(
        2026,
        8,
        1
    ),

    schedule=None,

    catchup=False,

    tags=[
        "retail",
        "data-quality",
        "snowflake",
        "monitoring",
    ],

    on_success_callback=
        quality_success_email,

) as dag:

    # ========================================================
    # START
    # ========================================================

    start = EmptyOperator(
        task_id="start"
    )

    # ========================================================
    # PARALLEL DATA QUALITY CHECKS
    # ========================================================

    customers_check = PythonOperator(

        task_id="check_customers",

        python_callable=check_customers,
    )

    products_check = PythonOperator(

        task_id="check_products",

        python_callable=check_products,
    )

    categories_check = PythonOperator(

        task_id="check_categories",

        python_callable=check_categories,
    )

    suppliers_check = PythonOperator(

        task_id="check_suppliers",

        python_callable=check_suppliers,
    )

    stores_check = PythonOperator(

        task_id="check_stores",

        python_callable=check_stores,
    )

    employees_check = PythonOperator(

        task_id="check_employees",

        python_callable=check_employees,
    )

    orders_check = PythonOperator(

        task_id="check_orders",

        python_callable=check_orders,
    )

    order_items_check = PythonOperator(

        task_id="check_order_items",

        python_callable=check_order_items,
    )

    payments_check = PythonOperator(

        task_id="check_payments",

        python_callable=check_payments,
    )

    shipments_check = PythonOperator(

        task_id="check_shipments",

        python_callable=check_shipments,
    )

    inventory_check = PythonOperator(

        task_id="check_inventory",

        python_callable=check_inventory,
    )

    reviews_check = PythonOperator(

        task_id="check_reviews",

        python_callable=check_reviews,
    )

    # ========================================================
    # QUALITY COMPLETE
    # ========================================================

    quality_complete = EmptyOperator(

        task_id="quality_complete"
    )

    # ========================================================
    # DEPENDENCIES
    #
    # ALL CHECKS START AFTER "start"
    # ALL CHECKS RUN IN PARALLEL
    # "quality_complete" waits for ALL checks
    # ========================================================

    start >> [
        customers_check,
        products_check,
        categories_check,
        suppliers_check,
        stores_check,
        employees_check,
        orders_check,
        order_items_check,
        payments_check,
        shipments_check,
        inventory_check,
        reviews_check,
    ] >> quality_complete