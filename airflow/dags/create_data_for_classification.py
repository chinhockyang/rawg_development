# ------------------------- Transform for Classification -------------------------

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

from transform_for_classification import *

# Set local machine data directory (Directory used to store classification data)
################################################################################################

def set_ml_data_directory(ti):

    # check if data directory exist, if not, create the folder
    if "data" not in os.listdir(root_dir):
        os.mkdir(os.path.join(root_dir, "data"))

    root_data_directory = os.path.join(root_dir, "data")

    # check if classificaton directory exist, if not, create the folder
    if "ml_data" not in os.listdir(root_data_directory):
        os.mkdir(os.path.join(root_data_directory, "ml_data"))
    
    # push the directory to descendent tasks
    data_directory = os.path.join(root_data_directory, "ml_data")
    ti.xcom_push("data_directory", data_directory)

# Create DAG Object
################################################################################################

with DAG(
    dag_id='transform_for_classification',
    schedule_interval=None,
    start_date=datetime(2023, 2, 1),
    catchup=False
) as dag:
    
    # Set New Games Data Directory
    task_set_data_directory = PythonOperator(
        task_id="set_data_directory",
        python_callable=set_ml_data_directory,
        provide_context=True
    )

    # Transform Game
    task_transform_game = PythonOperator(
        task_id='transform_game_data', 
        python_callable=transform_game_data, 
        provide_context=True)
    
    # Transform Platform
    task_transform_platform = PythonOperator(
        task_id='transform_platform_data', 
        python_callable=transform_platform_data, 
        provide_context=True)
    
    # Transform Store
    task_transform_store = PythonOperator(
        task_id='transform_store_data', 
        python_callable=transform_store_data, 
        provide_context=True)
    
    # Transform Genre
    task_transform_genre = PythonOperator(
        task_id='transform_genre_data', 
        python_callable=transform_genre_data, 
        provide_context=True)
    
    # Merge Data
    task_merge = PythonOperator(
        task_id='merge_data', 
        python_callable=merge_data, 
        provide_context=True)
    
    # Load Data
    task_load = PythonOperator(
        task_id='load_data', 
        python_callable=load_data, 
        provide_context=True)

    # Relationships
    task_transform_game >> task_transform_platform >> task_merge
    task_transform_game >> task_transform_store >> task_merge
    task_transform_game >> task_transform_genre >> task_merge
    task_merge >> task_load
    task_set_data_directory >> task_load