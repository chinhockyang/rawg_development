# ------------------------- Initial Upload ETL -------------------------

# Import Libraries
################################################################################################

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
from transform import *
from load import load_data

# Switch [Whether to perform Extraction]
################################################################################################

perform_extraction = True

def set_data_directory(**kwargs):
    ti = kwargs["ti"]
    initial_data_upload_directory = os.path.join(root_dir, "data", "initial_upload")
    ti.xcom_push("root_data_directory", initial_data_upload_directory)


# Create DAG Object
################################################################################################

with DAG(
    dag_id='initial_upload',
    schedule_interval=None,
    start_date=datetime(2023, 2, 1),
    catchup=False
) as dag:
    task_set_data_directory = PythonOperator(
        task_id="set_data_directory",
        python_callable=set_data_directory,
        provide_context=True
    )

    # Extract
    # =========================================================================

    if perform_extraction:
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

    # Tranform
    # =========================================================================

    task_transform_entity_game = PythonOperator(task_id='transform_entity_game', python_callable=transform_entity_game, provide_context=True)
    task_transform_entity_parent_platform = PythonOperator(task_id='transform_entity_parent_platform', python_callable=transform_entity_parent_platform, provide_context=True)
    task_transform_entity_platform = PythonOperator(task_id='transform_entity_platform', python_callable=transform_entity_platform, provide_context=True)
    task_transform_entity_publisher = PythonOperator(task_id='transform_entity_publisher', python_callable=transform_entity_publisher, provide_context=True)
    task_transform_entity_tag = PythonOperator(task_id='transform_entity_tag', python_callable=transform_entity_tag, provide_context=True)
    task_transform_entity_genre = PythonOperator(task_id='transform_entity_genre', python_callable=transform_entity_genre, provide_context=True)
    task_transform_entity_store = PythonOperator(task_id='transform_entity_store', python_callable=transform_entity_store, provide_context=True)
    task_transform_entity_rating = PythonOperator(task_id='transform_entity_rating', python_callable=transform_entity_rating, provide_context=True)
    task_transform_rs_game_platform = PythonOperator(task_id='transform_rs_game_platform', python_callable=transform_rs_game_platform, provide_context=True)
    task_transform_rs_game_genre = PythonOperator(task_id='transform_rs_game_genre', python_callable=transform_rs_game_genre, provide_context=True)
    task_transform_rs_game_store = PythonOperator(task_id='transform_rs_game_store', python_callable=transform_rs_game_store, provide_context=True)
    task_transform_rs_game_rating = PythonOperator(task_id='transform_rs_game_rating', python_callable=transform_rs_game_rating, provide_context=True)
    task_transform_rs_game_tag = PythonOperator(task_id='transform_rs_game_tag', python_callable=transform_rs_game_tag, provide_context=True)
    task_transform_rs_game_publisher = PythonOperator(task_id='transform_rs_game_publisher', python_callable=transform_rs_game_publisher, provide_context=True)
    
    # Load
    # =========================================================================

    task_load_data = PythonOperator(task_id='load_data', python_callable=load_data, provide_context=True)


    # Dependencies Configuration
    # =========================================================================

    if perform_extraction:
        task_set_data_directory >> task_extract_game_list >> task_extract_game_detail >> task_extract_publisher
        task_set_data_directory >> task_extract_publisher >> task_transform_entity_game >> task_load_data
        task_set_data_directory >> task_extract_publisher >> task_transform_entity_rating >> task_load_data
        task_set_data_directory >> task_extract_publisher >> task_transform_rs_game_platform >> task_load_data
        task_set_data_directory >> task_extract_publisher >> task_transform_rs_game_genre >> task_load_data
        task_set_data_directory >> task_extract_publisher >> task_transform_rs_game_store >> task_load_data
        task_set_data_directory >> task_extract_publisher >> task_transform_rs_game_rating >> task_load_data
        task_set_data_directory >> task_extract_publisher >> task_transform_rs_game_tag >> task_load_data
        task_set_data_directory >> task_extract_publisher >> task_transform_rs_game_publisher >> task_load_data
        task_set_data_directory >> task_extract_parent_platform >> task_transform_entity_parent_platform >> task_load_data
        task_set_data_directory >> task_extract_platform >> task_transform_entity_platform >> task_load_data
        task_set_data_directory >> task_extract_publisher >> task_transform_entity_publisher >> task_load_data
        task_set_data_directory >> task_extract_tag >> task_transform_entity_tag >> task_load_data
        task_set_data_directory >> task_extract_genre >> task_transform_entity_genre >> task_load_data
        task_set_data_directory >> task_extract_store >> task_transform_entity_store >> task_load_data
    else:
        task_set_data_directory >> task_transform_entity_game >> task_load_data
        task_set_data_directory >> task_transform_entity_rating >> task_load_data
        task_set_data_directory >> task_transform_rs_game_platform >> task_load_data
        task_set_data_directory >> task_transform_rs_game_genre >> task_load_data
        task_set_data_directory >> task_transform_rs_game_store >> task_load_data
        task_set_data_directory >> task_transform_rs_game_rating >> task_load_data
        task_set_data_directory >> task_transform_rs_game_tag >> task_load_data
        task_set_data_directory >> task_transform_rs_game_publisher >> task_load_data
        task_set_data_directory >> task_transform_entity_parent_platform >> task_load_data
        task_set_data_directory >> task_transform_entity_platform >> task_load_data
        task_set_data_directory >> task_transform_entity_publisher >> task_load_data
        task_set_data_directory >> task_transform_entity_tag >> task_load_data
        task_set_data_directory >> task_transform_entity_genre >> task_load_data
        task_set_data_directory >> task_transform_entity_store >> task_load_data
