from datetime import datetime, timedelta
from airflow import DAG, XComArg
from airflow.decorators import task_group
from airflow.operators.python import PythonOperator
from airflow.sensors.filesystem import FileSensor
import shutil
import time
import polars as pl
from pathlib import Path

default_args = {
    'owner': 'MITU',
    'retries': 5,
    'retry_delay': timedelta(minutes=2)
}

with DAG(
        dag_id='group_task',
        default_args=default_args,
        description='Check if dag works.',
        start_date=datetime(2024, 4, 20, 2),
        schedule_interval='@daily',
        catchup=False
) as dag:

    REGEX = '*orders_*.json'
    FOLDER_TO_SEARCH = 'orders'
    INPUT_FOLDER = Path('test_files/input/').joinpath(FOLDER_TO_SEARCH)
    STAGE_FOLDER = Path('test_files/stage/').joinpath(FOLDER_TO_SEARCH)
    BRONZE_FOLDER = Path('test_files/bronze/').joinpath(FOLDER_TO_SEARCH)
    SILVER_FOLDER = Path('test_files/silver/').joinpath(FOLDER_TO_SEARCH)
    OUTPUT_FOLDER = Path('test_files/output/').joinpath(FOLDER_TO_SEARCH)
    ORDERS_FILE_PATH = INPUT_FOLDER.joinpath(REGEX)

    def move_file(current_path, destination_path):
        shutil.move(current_path, destination_path)

    def get_matched_files(directory_path: Path, regex):
        files = directory_path.glob(regex)
        return files

    def move_matched_files_to_validation(ti, directory_path: Path, regex):
        files = get_matched_files(directory_path, regex)
        file_names = []
        for file in files:
            if file.exists():
                current_path = Path(file)
                file_name = current_path.name
                target_path = Path(STAGE_FOLDER).joinpath(file_name)
                file_names.append(file_name)
                move_file(current_path, target_path)
        ti.xcom_push(key="matched_files", value=file_names)
        file_names_dict_list = [{'file_name': file_name} for file_name in file_names]
        return file_names

    def validate_files(file_name):
        valid_files = []
        file_path = STAGE_FOLDER.joinpath(file_name)
        print(file_path)
        if file_path.exists():
            if str(file_path) == 'test_files/stage/orders/orders_2024_04_24.json':
                time.sleep(5)
            if str(file_path) == 'test_files/stage/orders/orders_2024_04_25.json':
                time.sleep(10)
            validate(file_path)
            valid_files.append(file_name)
            target_path = BRONZE_FOLDER.joinpath(file_name)
            print(f"Successful validation: {file_path}")
            move_file(file_path, target_path)
        return file_name

    def check_file_names(ti):
        file_names = ti.xcom_pull(
            # reference a task in a task group with task_group_id.task_id
            task_ids=["group1.transform_files"],
            key="return_value",
        )
        print(f"File names: {file_names}")


    def transform_files(file_name):
        transformed_files = []
        file_path = Path(BRONZE_FOLDER).joinpath(file_name)
        if file_path.exists():
            transform(file_path)
            transformed_files.append(file_name)
            target_path = Path(SILVER_FOLDER).joinpath(file_name)
            print(f"Successful transformation: {file_path}")
            move_file(file_path, target_path)
        return file_name

    def validate(file_path):
        print(f"Start validation of file: {file_path}")

    def transform(file_path):
        print(f"Start transformation of file: {file_path}")

    sensor_file_t = FileSensor(
        task_id="check_if_files_present",
        filepath=ORDERS_FILE_PATH,
        poke_interval=10,
        # I need to remember to set connection also for file. I can do that in GUI or in configuration
        fs_conn_id='order_file_conn_id',
        timeout=300,
    )

    move_files_t = PythonOperator(
        task_id='move_matched_files',
        python_callable=move_matched_files_to_validation,
        op_kwargs={"directory_path": INPUT_FOLDER, "regex": REGEX}
    )

    @task_group(group_id="group1")
    def tg1(file_name):

        validate_file_t = PythonOperator(
            task_id='validate_files',
            python_callable=validate_files,
            op_kwargs={"file_name": file_name})

        transform_file_t = PythonOperator(
            task_id='transform_files',
            python_callable=transform_files,
            op_kwargs={"file_name": XComArg(validate_file_t)})

        validate_file_t >> transform_file_t


    tg1_object = tg1.expand(file_name=XComArg(move_files_t))

    check_file_names_t = PythonOperator(
        task_id='check_file_names',
        python_callable=check_file_names
    )

    sensor_file_t >> move_files_t >> tg1_object >> check_file_names_t
