from datetime import datetime, timedelta

from airflow import DAG
from airflow.models import Variable
from airflow.providers.standard.operators.empty import EmptyOperator
from airflow.providers.standard.operators.bash import BashOperator
from airflow.utils.email import send_email


# ============================================================
# Airflow Variables
# ============================================================

DBT_PROJECT = Variable.get("retail_dbt_project")
ENVIRONMENT = Variable.get("retail_environment")


# ============================================================
# Email Failure Callback
# ============================================================

def task_failure_alert(context):

    # Get failed task information
    task_instance = context["task_instance"]

    dag_id = task_instance.dag_id
    task_id = task_instance.task_id
    logical_date = context["logical_date"]

    # Subject of email
    subject = f"Airflow Task Failed - {dag_id} - {task_id}"

    # Email body
    body = f"""
    <html>
    <body>

    <h2>Airflow Task Failure Alert</h2>

    <p><b>DAG ID:</b> {dag_id}</p>

    <p><b>Task ID:</b> {task_id}</p>

    <p><b>Environment:</b> {ENVIRONMENT}</p>

    <p><b>Logical Date:</b> {logical_date}</p>

    <p><b>Status:</b> FAILED</p>

    <br>

    <p>
    <b>Task Log:</b>
    <a href="{task_instance.log_url}">
        Open Airflow Task Log
    </a>
    </p>

    </body>
    </html>
    """

    # Send email
    send_email(
        to=["chakaliveeresh137@gmail.com"],
        subject=subject,
        html_content=body,
    )


# ============================================================
# Default arguments
# ============================================================

default_args = {
    "owner": "Veeresh",
    "depends_on_past": False,

    # Retry configuration
    "retries": 2,
    "retry_delay": timedelta(minutes=2),

    # Send email when task finally fails
    "on_failure_callback": task_failure_alert,
}


# ============================================================
# DAG definition
# ============================================================

with DAG(
    dag_id="retail_pipeline",

    description="Retail Data Pipeline using Fivetran + Snowflake + dbt",

    default_args=default_args,

    start_date=datetime(2026, 8, 1),

    schedule=None,

    catchup=False,

    tags=[
        "retail",
        "fivetran",
        "snowflake",
        "dbt"
    ],

) as dag:

    # ========================================================
    # Start
    # ========================================================

    start = EmptyOperator(
        task_id="start"
    )


    # ========================================================
    # DBT SOURCE FRESHNESS
    # ========================================================

    dbt_source_freshness = BashOperator(
        task_id="dbt_source_freshness",

        bash_command=f"""
        echo "Environment: {ENVIRONMENT}"
        echo "Running dbt source freshness"

        cd {DBT_PROJECT}

        dbt source freshness
        """,
    )


    # ========================================================
    # DBT SNAPSHOT
    # ========================================================

    dbt_snapshot = BashOperator(
        task_id="dbt_snapshot",

        bash_command=f"""
        echo "Environment: {ENVIRONMENT}"
        echo "Running dbt snapshot"

        cd {DBT_PROJECT}

        dbt snapshot
        """,
    )


    # ========================================================
    # DBT SEED
    # ========================================================

    dbt_seed = BashOperator(
        task_id="dbt_seed",

        bash_command=f"""
        echo "Environment: {ENVIRONMENT}"
        echo "Running dbt seed"

        cd {DBT_PROJECT}

        dbt seed
        """,
    )


    # ========================================================
    # DBT RUN
    # ========================================================

    dbt_run = BashOperator(
        task_id="dbt_run",

        bash_command=f"""
        echo "Environment: {ENVIRONMENT}"
        echo "Running dbt models"

        cd {DBT_PROJECT}

        dbt run
        """,
    )


    # ========================================================
    # DBT TEST
    # ========================================================

    dbt_test = BashOperator(
        task_id="dbt_test",

        bash_command=f"""
        echo "Environment: {ENVIRONMENT}"
        echo "Running dbt tests"

        cd {DBT_PROJECT}

        dbt test
        """,
    )


    # ========================================================
    # DBT DOCUMENTATION
    # ========================================================

    dbt_docs = BashOperator(
        task_id="dbt_docs_generate",

        bash_command=f"""
        echo "Environment: {ENVIRONMENT}"
        echo "Generating dbt documentation"

        cd {DBT_PROJECT}

        dbt docs generate
        """,
    )


    # ========================================================
    # End
    # ========================================================

    end = EmptyOperator(
        task_id="end"
    )


    # ========================================================
    # TASK DEPENDENCIES
    # ========================================================

    (
        start
        >> dbt_source_freshness
        >> dbt_snapshot
        >> dbt_seed
        >> dbt_run
        >> dbt_test
        >> dbt_docs
        >> end
    )