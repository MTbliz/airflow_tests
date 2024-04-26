from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator

default_args = {
    'owner': 'MITU',
    'retries': 5,
    'retry_delay': timedelta(minutes=2)
}

with DAG(
    dag_id='first_dag',
    default_args=default_args,
    description='Check if dag works.',
    start_date=datetime(2024, 4, 20, 2),
    schedule_interval='@daily'
) as dag:

    t1 = BashOperator(
        task_id='first_task',
        bash_command='echo hello world'
    )
