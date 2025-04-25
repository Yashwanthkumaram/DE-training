from airflow import DAG
from airflow.providers.postgres.operators.postgres import PostgresOperator
from datetime import datetime

with DAG(
    dag_id='insert_users',
    start_date=datetime(2024, 1, 1),
) as dag:

    insert_data = PostgresOperator(
        task_id='insert_data',
        postgres_conn_id='my_postgres',
        sql="""
        INSERT INTO users (name, email)
        VALUES 
            ('Yash', 'yash@galepartners.com'),
            ('Jeevan', 'jeevan@galepartners.com'),
            ('yashas', 'yashas@galepartners.com');
        """
    )
