from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime

def my_func():
    print("Jeevan")

with DAG(
    dag_id="my_hello_world",
    start_date=datetime(2024, 1, 1),



) as dag:
    say_hello = PythonOperator(
    task_id ="saying_hello",
    python_callable = my_func

    )
    
