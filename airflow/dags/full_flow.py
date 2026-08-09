from datetime import datetime, timedelta

from airflow import DAG
from airflow.models import Variable
from airflow.utils.email import send_email

from airflow.providers.standard.operators.empty import EmptyOperator
from airflow.providers.standard.operators.bash import BashOperator
from airflow.providers.standard.operators.python import PythonOperator

from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook


# ============================================================
# AIRFLOW VARIABLES
# ============================================================

DBT_PROJECT = Variable.get("retail_dbt_project")
ENVIRONMENT = Variable.get("retail_environment")


# ============================================================
# EMAIL CONFIGURATION
# ============================================================

ALERT_EMAIL = "chakaliveeresh137@gmail.com"


# ============================================================
# FAILURE CALLBACK
# ============================================================

def task_failure_alert(context):

    task_instance = context["task_instance"]

    dag_id = task_instance.dag_id
    task_id = task_instance.task_id
    run_id = task_instance.run_id
    try_number = task_instance.try_number

    exception = context.get("exception")

    if exception:
        error_message = str(exception)
    else:
        error_message = "Unknown error"

    log_url = task_instance.log_url

    subject = (
        f"FAILED: Airflow DAG {dag_id} | Task {task_id}"
    )

    html_content = f"""
    <html>
    <body>

        <h2>❌ Airflow Task Failed</h2>

        <table border="1" cellpadding="6" cellspacing="0">

            <tr>
                <td><b>DAG</b></td>
                <td>{dag_id}</td>
            </tr>

            <tr>
                <td><b>Task</b></td>
                <td>{task_id}</td>
            </tr>

            <tr>
                <td><b>Run ID</b></td>
                <td>{run_id}</td>
            </tr>

            <tr>
                <td><b>Environment</b></td>
                <td>{ENVIRONMENT}</td>
            </tr>

            <tr>
                <td><b>Attempt</b></td>
                <td>{try_number}</td>
            </tr>

            <tr>
                <td><b>Error</b></td>
                <td>
                    <pre>{error_message}</pre>
                </td>
            </tr>

        </table>

        <br>

        <p>
            <b>Task Log:</b>
            <a href="{log_url}">Open Airflow Log</a>
        </p>

        <p>
            Please investigate the failed task.
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
# DAG SUCCESS CALLBACK
# ============================================================

def dag_success_alert(context):

    dag_run = context["dag_run"]

    dag_id = dag_run.dag_id
    run_id = dag_run.run_id

    subject = (
        f"SUCCESS: Airflow DAG {dag_id} Completed"
    )

    html_content = f"""
    <html>
    <body>

        <h2>✅ Retail Data Pipeline Completed Successfully</h2>

        <table border="1" cellpadding="6" cellspacing="0">

            <tr>
                <td><b>DAG</b></td>
                <td>{dag_id}</td>
            </tr>

            <tr>
                <td><b>Run ID</b></td>
                <td>{run_id}</td>
            </tr>

            <tr>
                <td><b>Environment</b></td>
                <td>{ENVIRONMENT}</td>
            </tr>

            <tr>
                <td><b>Status</b></td>
                <td>SUCCESS</td>
            </tr>

        </table>

        <br>

        <h3>Pipeline Steps Completed</h3>

        <ul>
            <li>Fivetran RAW data validation</li>
            <li>dbt source freshness</li>
            <li>dbt seed</li>
            <li>dbt snapshot</li>
            <li>dbt models</li>
            <li>dbt tests</li>
            <li>dbt documentation generation</li>
        </ul>

        <p>
            The complete retail data pipeline finished successfully.
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
# FIVETRAN RAW DATA VALIDATION
# ============================================================

def check_fivetran_data():

    hook = SnowflakeHook(
        snowflake_conn_id="snowflake_retail"
    )

    sql = """
        SELECT
            COUNT(*)
        FROM RETAIL_DB.RAW.ORDERS
        WHERE _FIVETRAN_SYNCED >= DATEADD(
            hour,
            -50,
            CURRENT_TIMESTAMP()
        )
    """

    result = hook.get_first(sql)

    record_count = result[0]

    print(
        f"Fivetran records loaded in last 12 hours: "
        f"{record_count}"
    )

    if record_count == 0:

        raise ValueError(
            "FIVETRAN DATA VALIDATION FAILED: "
            "No records were found in "
            "RETAIL_DB.RAW.ORDERS during the last 12 hours."
        )

    print(
        "Fivetran RAW data validation successful."
    )


# ============================================================
# DEFAULT ARGUMENTS
# ============================================================

default_args = {

    "owner": "Veeresh",

    "depends_on_past": False,

    "retries": 2,

    "retry_delay": timedelta(minutes=2),

    "email": [ALERT_EMAIL],

    "email_on_failure": False,

    "email_on_retry": False,

    "on_failure_callback": task_failure_alert,
}


# ============================================================
# DAG
# ============================================================

with DAG(

    dag_id="full_flow",

    description=(
        "Retail Data Pipeline using "
        "Fivetran + Snowflake + dbt"
    ),

    default_args=default_args,

    start_date=datetime(2026, 8, 1),

    schedule=None,

    catchup=False,

    dagrun_timeout=timedelta(hours=2),

    on_success_callback=dag_success_alert,

    tags=[
        "retail",
        "fivetran",
        "snowflake",
        "dbt",
        "production",
    ],

) as dag:

    # ========================================================
    # START
    # ========================================================

    start = EmptyOperator(
        task_id="start"
    )


    # ========================================================
    # FIVETRAN DATA CHECK
    # ========================================================

    check_fivetran = PythonOperator(

        task_id="check_fivetran_data",

        python_callable=check_fivetran_data,

        execution_timeout=timedelta(
            minutes=10
        ),
    )


    # ========================================================
    # DBT SOURCE FRESHNESS
    # ========================================================

    dbt_source_freshness = BashOperator(

        task_id="dbt_source_freshness",

        bash_command=f"""

        echo "=========================================="
        echo "Environment: {ENVIRONMENT}"
        echo "Running dbt source freshness"
        echo "=========================================="

        cd {DBT_PROJECT}

        dbt source freshness

        """,

        execution_timeout=timedelta(
            minutes=20
        ),
    )


    # ========================================================
    # DBT SEED
    # ========================================================

    dbt_seed = BashOperator(

        task_id="dbt_seed",

        bash_command=f"""

        echo "=========================================="
        echo "Environment: {ENVIRONMENT}"
        echo "Running dbt seed"
        echo "=========================================="

        cd {DBT_PROJECT}

        dbt seed

        """,

        execution_timeout=timedelta(
            minutes=20
        ),
    )


    # ========================================================
    # DBT SNAPSHOT
    # ========================================================

    dbt_snapshot = BashOperator(

        task_id="dbt_snapshot",

        bash_command=f"""

        echo "=========================================="
        echo "Environment: {ENVIRONMENT}"
        echo "Running dbt snapshot"
        echo "=========================================="

        cd {DBT_PROJECT}

        dbt snapshot

        """,

        execution_timeout=timedelta(
            minutes=30
        ),
    )


    # ========================================================
    # DBT RUN
    # ========================================================

    dbt_run = BashOperator(

        task_id="dbt_run",

        bash_command=f"""

        echo "=========================================="
        echo "Environment: {ENVIRONMENT}"
        echo "Running dbt models"
        echo "=========================================="

        cd {DBT_PROJECT}

        dbt run

        """,

        execution_timeout=timedelta(
            minutes=45
        ),
    )


    # ========================================================
    # DBT TEST
    # ========================================================

    dbt_test = BashOperator(

        task_id="dbt_test",

        bash_command=f"""

        echo "=========================================="
        echo "Environment: {ENVIRONMENT}"
        echo "Running dbt tests"
        echo "=========================================="

        cd {DBT_PROJECT}

        dbt test

        """,

        execution_timeout=timedelta(
            minutes=30
        ),
    )


    # ========================================================
    # DBT DOCUMENTATION
    # ========================================================

    dbt_docs = BashOperator(

        task_id="dbt_docs_generate",

        bash_command=f"""

        echo "=========================================="
        echo "Environment: {ENVIRONMENT}"
        echo "Generating dbt documentation"
        echo "=========================================="

        cd {DBT_PROJECT}

        dbt docs generate

        """,

        execution_timeout=timedelta(
            minutes=20
        ),
    )


    # ========================================================
    # END
    # ========================================================

    end = EmptyOperator(
        task_id="end"
    )


    # ========================================================
    # DEPENDENCIES
    # ========================================================

    (
        start
        >> check_fivetran
        >> dbt_source_freshness
        >> dbt_seed
        >> dbt_snapshot
        >> dbt_run
        >> dbt_test
        >> dbt_docs
        >> end
    )