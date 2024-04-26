from datetime import datetime, timedelta
from airflow import DAG, XComArg
from airflow.operators.python import PythonOperator
from airflow.sensors.filesystem import FileSensor
import shutil
import glob
import polars as pl
from pathlib import Path

default_args = {
    'owner': 'MITU',
    'retries': 5,
    'retry_delay': timedelta(minutes=2)
}

with DAG(
        dag_id='shop_orders',
        default_args=default_args,
        description='Check if dag works.',
        start_date=datetime(2024, 4, 20, 2),
        schedule_interval='@daily',
        catchup=False
) as dag:

    INPUT_FOLDER = 'test_files/input/orders'
    STAGE_FOLDER = 'test_files/stage/orders'
    BRONZE_FOLDER = 'test_files/bronze/orders'
    SILVER_FOLDER = 'test_files/silver/orders'
    OUTPUT_FOLDER = 'test_files/output/orders'
    ORDERS_FILE_PATH = INPUT_FOLDER + '/*orders_*.json'

    def move_file(current_path, destination_path):
        shutil.move(current_path, destination_path)

    def get_matched_files(regex):
        files = glob.glob(regex)
        return files

    def validate_orders(ti):
        file_names = ti.xcom_pull(key="matched_files", task_ids="first_task")
        valid_orders = []
        for file_name in file_names:
            file_path = Path(STAGE_FOLDER).joinpath(file_name)
            valid_orders.append(file_name)
            target_path = Path(BRONZE_FOLDER).joinpath(file_name)
            print(f"Success validation: {file_path}")
            print(f"file_names: {file_names}")
            move_file(file_path, target_path)
        ti.xcom_push(key="valid_orders", value=valid_orders)

    def transform_orders(ti):
        file_names = ti.xcom_pull(key="valid_orders", task_ids="validate_orders")
        transformed_orders = []
        for file_name in file_names:
            file_path = Path(BRONZE_FOLDER).joinpath(file_name)
            transformed_orders.append(file_name)
            target_path = Path(SILVER_FOLDER).joinpath(file_name)
            print(f"Success transform: {file_path}")
            print(f"file_names: {file_names}")
            move_file(file_path, target_path)
        ti.xcom_push(key="transformed_orders", value=transformed_orders)

    def move_matched_files(ti, regex):
        files = get_matched_files(regex)
        file_names = []
        for file in files:
            current_path = Path(file)
            file_name = current_path.name
            target_path = Path(STAGE_FOLDER).joinpath(file_name)
            file_names.append(file_name)
            move_file(current_path, target_path)
        ti.xcom_push(key="matched_files", value=file_names)

    sensor_file_t = FileSensor(
        task_id="check_orders_file",
        filepath=ORDERS_FILE_PATH,
        poke_interval=10,
        # I need to remember to set connection also for file. I can do that in GUI or in configuration
        fs_conn_id='order_file_conn_id',
        timeout=300,
    )

    move_files_t = PythonOperator(
        task_id='first_task',
        python_callable=move_matched_files,
        op_kwargs={"regex": ORDERS_FILE_PATH}
    )

    validate_orders_t = PythonOperator(
        task_id='validate_orders',
        python_callable=validate_orders
    )

    transform_orders_t = PythonOperator(
        task_id='transform_orders',
        python_callable=transform_orders
    )

    sensor_file_t >> move_files_t >> validate_orders_t >> transform_orders_t
