from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
import polars as pl

default_args = {
    'owner': 'MITU',
    'retries': 5,
    'retry_delay': timedelta(minutes=2)
}

with DAG(
        dag_id='polars_test_dag',
        default_args=default_args,
        description='Check if dag works.',
        start_date=datetime(2024, 4, 20, 2),
        schedule_interval='@daily',
        catchup=False
) as dag:

    def test_polars():
        df = pl.DataFrame(
            {
                "A": [1, 2, 3, 4, 5],
                "fruits": ["banana", "banana", "apple", "apple", "banana"],
                "B": [5, 4, 3, 2, 1],
                "cars": ["beetle", "audi", "beetle", "beetle", "beetle"],
            })
        print(df.head())


    t1 = PythonOperator(
        task_id='first_task',
        python_callable=test_polars
    )
