# ------------------------- Monthly Upload ETL -------------------------

# Import Libraries
################################################################################################
import requests
from datetime import datetime
from airflow.models import DAG
from airflow.operators.python import PythonOperator

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
    dag_id='monthly_upload',
    schedule_interval='@monthly',
    start_date=datetime(2023, 2, 1),
    catchup=False
) as dag:

    task_extract_game_list = PythonOperator(
        task_id="extract_new_game_list",
        python_callable=extract_game_list,
        provide_context=True,
        op_kwargs={
            'extraction_task': 'extract_new_games',
            'year': datetime.today().year,
            'month': datetime.today().month
        }
    )

    task_extract_game_list = PythonOperator(
        task_id="extract_updated_game_list",
        python_callable=extract_game_list,
        provide_context=True,
        op_kwargs={
            'extraction_task': 'extract_updates',
            'year': datetime.today().year,
            'month': datetime.today().month
        }
    )