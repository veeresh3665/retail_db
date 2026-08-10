from datetime import datetime, timedelta
import time

import requests
from requests.auth import HTTPBasicAuth

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

ENVIRONMENT = Variable.get(
    "retail_environment",
    default_var="dev"
)

FIVETRAN_CONNECTION_ID = Variable.get(
    "fivetran_connector_id"
)


# ============================================================
# AIRFLOW CONNECTION
# ============================================================

FIVETRAN_CONN_ID = "fivetran_api"

SNOWFLAKE_CONN_ID = "snowflake_retail"


# ============================================================
# EMAIL
# ============================================================

ALERT_EMAIL = "chakaliveeresh137@gmail.com"


# ============================================================
# FIVETRAN API HELPERS
# ============================================================

def get_fivetran_credentials():

    from airflow.hooks.base import BaseHook

    connection = BaseHook.get_connection(
        FIVETRAN_CONN_ID
    )

    api_key = connection.login
    api_secret = connection.password

    return api_key, api_secret


def fivetran_request(
    method,
    endpoint,
    payload=None
):

    api_key, api_secret = (
        get_fivetran_credentials()
    )

    url = (
        "https://api.fivetran.com/v1"
        + endpoint
    )

    response = requests.request(

        method=method,

        url=url,

        auth=HTTPBasicAuth(
            api_key,
            api_secret
        ),

        headers={
            "Accept": "application/json",
            "Content-Type": "application/json",
        },

        json=payload,

        timeout=60,
    )

    if not response.ok:

        raise RuntimeError(
            f"Fivetran API failed. "
            f"HTTP {response.status_code}. "
            f"Response: {response.text}"
        )

    return response.json()


# ============================================================
# TRIGGER FIVETRAN
# ============================================================

def trigger_fivetran_sync():

    print(
        f"Triggering Fivetran connection: "
        f"{FIVETRAN_CONNECTION_ID}"
    )

    result = fivetran_request(

        method="POST",

        endpoint=(
            f"/connections/"
            f"{FIVETRAN_CONNECTION_ID}/sync"
        ),

        payload={
            "force": False
        }
    )

    print("Fivetran sync trigger response:")

    print(result)

    print(
        "Fivetran sync successfully triggered."
    )


# ============================================================
# WAIT FOR FIVETRAN
# ============================================================

def wait_for_fivetran():

    print(
        "Waiting for Fivetran sync to complete..."
    )

    max_wait_minutes = 60

    poll_seconds = 30

    max_attempts = int(
        (max_wait_minutes * 60)
        / poll_seconds
    )

    for attempt in range(
        1,
        max_attempts + 1
    ):

        result = fivetran_request(

            method="GET",

            endpoint=(
                f"/connections/"
                f"{FIVETRAN_CONNECTION_ID}"
            )
        )

        data = result.get(
            "data",
            {}
        )

        status = data.get(
            "status",
            {}
        )

        sync_state = status.get(
            "sync_state"
        )

        update_state = status.get(
            "update_state"
        )

        print(
            f"Attempt {attempt}: "
            f"sync_state={sync_state}, "
            f"update_state={update_state}"
        )

        # ----------------------------------------------------
        # SUCCESS
        # ----------------------------------------------------

        if sync_state == "scheduled":

            print(
                "Fivetran sync completed "
                "and connection is scheduled."
            )

            return


        # ----------------------------------------------------
        # FAILURE STATES
        # ----------------------------------------------------

        if sync_state == "paused":

            raise RuntimeError(
                "Fivetran connection is PAUSED."
            )


        # ----------------------------------------------------
        # STILL RUNNING
        # ----------------------------------------------------

        if sync_state == "syncing":

            print(
                "Fivetran sync is still running. "
                "Waiting..."
            )

            time.sleep(
                poll_seconds
            )

            continue


        # ----------------------------------------------------
        # UNKNOWN STATE
        # ----------------------------------------------------

        print(
            f"Unexpected Fivetran state: "
            f"{sync_state}"
        )

        time.sleep(
            poll_seconds
        )


    # --------------------------------------------------------
    # TIMEOUT
    # --------------------------------------------------------

    raise TimeoutError(
        "Fivetran sync did not complete "
        f"within {max_wait_minutes} minutes."
    )


# ============================================================
# VALIDATE SNOWFLAKE RAW
# ============================================================

def check_fivetran_data():

    hook = SnowflakeHook(
        snowflake_conn_id=SNOWFLAKE_CONN_ID
    )

    sql = """

        SELECT COUNT(*)

        FROM RETAIL_DB.RAW.ORDERS

        WHERE _FIVETRAN_SYNCED >= DATEADD(
            hour,
            -500,
            CURRENT_TIMESTAMP()
        )

    """

    result = hook.get_first(sql)

    record_count = result[0]

    print(
        f"Recent Fivetran records: "
        f"{record_count}"
    )

    if record_count == 0:

        raise ValueError(

            "FIVETRAN RAW VALIDATION FAILED. "

            "No recent records found in "
            "RETAIL_DB.RAW.ORDERS "

            "during the last 12 hours."
        )

    print(
        "Snowflake RAW validation successful."
    )


# ============================================================
# FAILURE EMAIL
# ============================================================

def task_failure_alert(context):

    task_instance = (
        context["task_instance"]
    )

    dag_id = task_instance.dag_id

    task_id = task_instance.task_id

    run_id = task_instance.run_id

    try_number = (
        task_instance.try_number
    )

    exception = context.get(
        "exception"
    )

    if exception:

        error_message = str(
            exception
        )

    else:

        error_message = (
            "Unknown error"
        )

    log_url = (
        task_instance.log_url
    )

    subject = (
        f"❌ FAILED | "
        f"{dag_id} | {task_id}"
    )

    html_content = f"""

    <html>

    <body>

        <h2>
            ❌ Airflow Pipeline Failure
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
                    <pre>
{error_message}
                    </pre>
                </td>

            </tr>

        </table>

        <br>

        <b>Airflow Log:</b>

        <a href="{log_url}">
            Open Task Log
        </a>

    </body>

    </html>

    """

    send_email(

        to=[ALERT_EMAIL],

        subject=subject,

        html_content=html_content
    )


# ============================================================
# SUCCESS EMAIL
# ============================================================

def dag_success_alert(context):

    dag_run = context["dag_run"]

    dag_id = dag_run.dag_id

    run_id = dag_run.run_id

    subject = (
        f"✅ SUCCESS | "
        f"{dag_id}"
    )

    html_content = f"""

    <html>

    <body>

        <h2>
            ✅ Retail Data Pipeline
            Completed Successfully
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

        <h3>
            Pipeline Stages
        </h3>

        <ul>

            <li>
                Fivetran Sync
            </li>

            <li>
                Snowflake RAW Validation
            </li>

            <li>
                dbt Source Freshness
            </li>

            <li>
                dbt Seed
            </li>

            <li>
                dbt Snapshot
            </li>

            <li>
                dbt Run
            </li>

            <li>
                dbt Test
            </li>

            <li>
                dbt Documentation
            </li>

        </ul>

        <p>
            Complete pipeline executed
            successfully.
        </p>

    </body>

    </html>

    """

    send_email(

        to=[ALERT_EMAIL],

        subject=subject,

        html_content=html_content
    )


# ============================================================
# DEFAULT ARGS
# ============================================================

default_args = {

    "owner": "Veeresh",

    "depends_on_past": False,

    "retries": 2,

    "retry_delay": timedelta(
        minutes=2
    ),

    "email_on_failure": False,

    "email_on_retry": False,

    "on_failure_callback":
        task_failure_alert,
}


# ============================================================
# DAG
# ============================================================

with DAG(

    dag_id="fivetran_dag",

    description=(
        "Fivetran + Snowflake + dbt "
        "Retail Data Pipeline"
    ),

    default_args=default_args,

    start_date=datetime(
        2026,
        8,
        1
    ),

    schedule=None,

    catchup=False,

    dagrun_timeout=timedelta(
        hours=2
    ),

    on_success_callback=
        dag_success_alert,

    tags=[
        "retail",
        "fivetran",
        "snowflake",
        "dbt",
        "etl",
    ],

) as dag:


    # ========================================================
    # START
    # ========================================================

    start = EmptyOperator(

        task_id="start"
    )


    # ========================================================
    # TRIGGER FIVETRAN
    # ========================================================

    trigger_fivetran = PythonOperator(

        task_id="trigger_fivetran",

        python_callable=
            trigger_fivetran_sync,

        execution_timeout=
            timedelta(
                minutes=10
            ),
    )


    # ========================================================
    # WAIT FOR FIVETRAN
    # ========================================================

    wait_for_fivetran_sync = PythonOperator(

        task_id="wait_for_fivetran",

        python_callable=
            wait_for_fivetran,

        execution_timeout=
            timedelta(
                minutes=70
            ),
    )


    # ========================================================
    # VALIDATE RAW
    # ========================================================

    validate_raw = PythonOperator(

        task_id="validate_fivetran_raw",

        python_callable=
            check_fivetran_data,

        execution_timeout=
            timedelta(
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

        echo "Running dbt source freshness"

        echo "=========================================="

        cd {DBT_PROJECT}

        dbt source freshness

        """,

        execution_timeout=
            timedelta(
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

        echo "Running dbt seed"

        echo "=========================================="

        cd {DBT_PROJECT}

        dbt seed

        """,

        execution_timeout=
            timedelta(
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

        echo "Running dbt snapshot"

        echo "=========================================="

        cd {DBT_PROJECT}

        dbt snapshot

        """,

        execution_timeout=
            timedelta(
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

        echo "Running dbt models"

        echo "=========================================="

        cd {DBT_PROJECT}

        dbt run

        """,

        execution_timeout=
            timedelta(
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

        echo "Running dbt tests"

        echo "=========================================="

        cd {DBT_PROJECT}

        dbt test

        """,

        execution_timeout=
            timedelta(
                minutes=30
            ),
    )


    # ========================================================
    # DBT DOCS
    # ========================================================

    dbt_docs = BashOperator(

        task_id="dbt_docs_generate",

        bash_command=f"""

        echo "=========================================="

        echo "Generating dbt documentation"

        echo "=========================================="

        cd {DBT_PROJECT}

        dbt docs generate

        """,

        execution_timeout=
            timedelta(
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
    # PIPELINE
    # ========================================================

    (

        start

        >> trigger_fivetran

        >> wait_for_fivetran_sync

        >> validate_raw

        >> dbt_source_freshness

        >> dbt_seed

        >> dbt_snapshot

        >> dbt_run

        >> dbt_test

        >> dbt_docs

        >> end

    )