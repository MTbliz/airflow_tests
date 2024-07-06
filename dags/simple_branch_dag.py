"""Example DAG demonstrating the usage of the BranchPythonOperator."""

from airflow.models.dag import DAG
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import BranchPythonOperator
from airflow.utils.edgemodifier import Label

import random
from pendulum import datetime

with DAG(
    dag_id='branch_python_operator_example',
    start_date=datetime(2023, 1, 1),
    catchup=False,
    schedule="@daily"
) as dag:

    run_this_first = EmptyOperator(
        task_id='run_this_first',
    )

    options = ['branch_a', 'branch_b', 'branch_c', 'branch_d']

    branching = BranchPythonOperator(
        task_id='branching',
        python_callable=lambda: random.choice(options),
    )

    run_this_first >> branching

    join = EmptyOperator(
        task_id='join',
        trigger_rule="all_success",
    )

    for option in options:

        t = EmptyOperator(
            task_id=option,
        )

        empty_follow = EmptyOperator(
            task_id='follow_' + option,
        )

        # Label is optional here, but it can help identify more complex branches
        branching >> Label(option) >> t >> empty_follow >> join