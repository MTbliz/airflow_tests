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
        dag_id='etl_template',
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

    def validate_files(ti):
        file_names = ti.xcom_pull(key="matched_files", task_ids="move_matched_files")
        valid_files = []
        for file_name in file_names:
            file_path = STAGE_FOLDER.joinpath(file_name)
            if file_path.exists():
                validate(file_path)
                valid_files.append(file_name)
                target_path = BRONZE_FOLDER.joinpath(file_name)
                print(f"Successful validation: {file_path}")
                move_file(file_path, target_path)
        ti.xcom_push(key="valid_files", value=valid_files)

    def transform_files(ti):
        file_names = ti.xcom_pull(key="valid_files", task_ids="validate_files")
        transformed_files = []
        for file_name in file_names:
            file_path = Path(BRONZE_FOLDER).joinpath(file_name)
            if file_path.exists():
                transform(file_path)
                transformed_files.append(file_name)
                target_path = Path(SILVER_FOLDER).joinpath(file_name)
                print(f"Successful transformation: {file_path}")
                move_file(file_path, target_path)
        ti.xcom_push(key="transformed_files", value=transformed_files)

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

    validate_files_t = PythonOperator(
        task_id='validate_files',
        python_callable=validate_files
    )

    transform_files_t = PythonOperator(
        task_id='transform_files',
        python_callable=transform_files
    )

    sensor_file_t >> move_files_t >> validate_files_t >> transform_files_t
