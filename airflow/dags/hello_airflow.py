from datetime import datetime

from airflow import DAG
from airflow.providers.standard.operators.empty import EmptyOperator
from airflow.providers.standard.operators.bash import BashOperator
from airflow.providers.standard.operators.python import PythonOperator

def hello():
    print("================================")
    print("Hello Airflow!")
    print("My first DAG is running.")
    print("================================")


def welcome():
    print("================================")
    print("Welcome to Apache Airflow")
    print("================================")


with DAG(
    dag_id="hello_airflow",
    description="My First Airflow DAG",
    start_date=datetime(2026, 1,1),
    schedule=None,
    catchup=False,
    tags=["training"],
) as dag:

    start = EmptyOperator(
        task_id="start"
    )

    hello_task = PythonOperator(
        task_id="hello_task",
        python_callable=hello
    )

    welcome_task = PythonOperator(
        task_id="welcome_task",
        python_callable=welcome
    )

    end = EmptyOperator(
        task_id="end"
    )

    start >> hello_task >> welcome_task >> end