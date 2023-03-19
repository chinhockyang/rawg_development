# ------------------------- Initial Upload ETL -------------------------

# Import Libraries
################################################################################################
import requests
from datetime import datetime
from airflow.models import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash_operator import BashOperator

# Load Python Operators
################################################################################################

import os
import sys
dags_dir = os.path.dirname(os.path.abspath(__file__))
airflow_dir = os.path.dirname(dags_dir)
root_dir = os.path.dirname(airflow_dir)
sys.path.append(root_dir)

from extract import *

# Create DAG Object
################################################################################################

with DAG(
    dag_id='initial_upload',
    schedule_interval=None,
    start_date=datetime(2023, 2, 1),
    catchup=False
) as dag:

    task_extract_game_list = PythonOperator(
        task_id="extract_game_list",
        python_callable=extract_game_list,
        provide_context=True,
        op_kwargs={'extraction_task': 'initial_upload'},
    )

    task_extract_game_detail = PythonOperator(
        task_id="extract_game_detail",
        python_callable=extract_game_detail,
        provide_context=True
    )

    task_extract_publisher = PythonOperator(
        task_id="extract_publisher",
        python_callable=extract_publisher,
        provide_context=True
    )

    task_extract_genre = PythonOperator(
        task_id="extract_genre",
        python_callable=extract_genre,
        provide_context=True
    )

    task_extract_tag = PythonOperator(
        task_id="extract_tag",
        python_callable=extract_tag,
        provide_context=True
    )

    task_extract_store = PythonOperator(
        task_id="extract_store",
        python_callable=extract_store,
        provide_context=True
    )

    task_extract_platform = PythonOperator(
        task_id="extract_platform",
        python_callable=extract_platform,
        provide_context=True
    )

    task_extract_parent_platform = PythonOperator(
        task_id="extract_parent_platform",
        python_callable=extract_parent_platform,
        provide_context=True
    )

    task_extract_game_list >> task_extract_game_detail >> task_extract_publisher