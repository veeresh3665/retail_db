from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator

from datetime import datetime


def hello():
    print("Hello Airflow")
    print("Welcome to Data Engineering")


with DAG(
    dag_id="hello_airflow",
    start_date=datetime(2026, 1, 1),
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

    end = EmptyOperator(
        task_id="end"
    )

    start >> hello_task >> end