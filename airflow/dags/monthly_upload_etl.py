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
from transform import *

# Load Python Operators
################################################################################################

import os
import sys
dags_dir = os.path.dirname(os.path.abspath(__file__))
airflow_dir = os.path.dirname(dags_dir)
root_dir = os.path.dirname(airflow_dir)
sys.path.append(root_dir)

from extract import *


def set_new_games_data_directory(**kwargs):
    ti = kwargs["ti"]
    data_directory = os.path.join(root_dir, "data", "monthly_upload", "new_games")
    ti.xcom_push("root_data_directory", data_directory)



# Create DAG Object
################################################################################################

with DAG(
    dag_id='monthly_upload',
    schedule_interval='@monthly',
    start_date=datetime(2023, 2, 1),
    catchup=False
) as dag:
    # Set New Games Data Directory
    task_set_data_directory = PythonOperator(
        task_id="set_data_directory",
        python_callable=set_new_games_data_directory,
        provide_context=True
    )

    # Extract Games
    # =========================================================================
    
    # Extract New Game List
    task_extract_new_game_list = PythonOperator(
        task_id="extract_new_game_list",
        python_callable=extract_game_list,
        provide_context=True,
        op_kwargs={
            'extraction_task': 'extract_new_games',
            'year': datetime.today().year,
            'month': datetime.today().month
        }
    )

    # Extract New Game Detail
    task_extract_game_detail = PythonOperator(
            task_id="extract_new_game_detail",
            python_callable=extract_game_detail,
            provide_context=True
    )

    
    # Check for New Information (Genre, Tags etc.)
    # =========================================================================
    
    task_check_new_publisher = PythonOperator(
            task_id="check_new_publisher",
            python_callable=check_new_records,
            provide_context=True,
            op_kwargs={
                'entity': 'publisher',
                'file_name': 'game_details_publisher.csv'
            }
    )

    task_check_new_genre = PythonOperator(
            task_id="check_new_genre",
            python_callable=check_new_records,
            provide_context=True,
            op_kwargs={
                'entity': 'genre',
                'file_name': 'game_genre.csv'
            }
    )

    task_check_new_tag = PythonOperator(
            task_id="check_new_tag",
            python_callable=check_new_records,
            provide_context=True,
            op_kwargs={
                'entity': 'tag',
                'file_name': 'game_tag.csv'
            }
    )

    task_check_new_store = PythonOperator(
            task_id="check_new_store",
            python_callable=check_new_records,
            provide_context=True,
            op_kwargs={
                'entity': 'store',
                'file_name': 'game_store.csv'
            }
    )

    task_check_new_platform = PythonOperator(
            task_id="check_new_platform",
            python_callable=check_new_records,
            provide_context=True,
            op_kwargs={
                'entity': 'platform',
                'file_name': 'game_platform.csv'
            }
    )

    # Extract New Information (Genre, Tags etc.)
    # =========================================================================

    task_extract_publisher = PythonOperator(
            task_id="extract_publisher",
            python_callable=extract_publisher,
            provide_context=True
    )

    task_extract_genre = PythonOperator(
            task_id="extract_genre",
            python_callable=extract_publisher,
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

    # Transform New Information (Genre, Tags etc.)
    # =========================================================================
    task_transform_entity_game = PythonOperator(task_id='transform_entity_game', python_callable=transform_entity_game, provide_context=True)
    task_transform_entity_publisher = PythonOperator(task_id='transform_entity_publisher', python_callable=transform_entity_publisher, provide_context=True)
    task_transform_entity_genre = PythonOperator(task_id='transform_entity_genre', python_callable=transform_entity_genre, provide_context=True)
    task_transform_entity_tag = PythonOperator(task_id='transform_entity_tag', python_callable=transform_entity_tag, provide_context=True)
    task_transform_entity_store = PythonOperator(task_id='transform_entity_store', python_callable=transform_entity_store, provide_context=True)
    task_transform_entity_platform = PythonOperator(task_id='transform_entity_platform', python_callable=transform_entity_platform, provide_context=True)
    task_transform_entity_rating = PythonOperator(task_id='transform_entity_rating', python_callable=transform_entity_rating, provide_context=True)
    task_transform_rs_game_platform = PythonOperator(task_id='transform_rs_game_platform', python_callable=transform_rs_game_platform, provide_context=True)
    task_transform_rs_game_genre = PythonOperator(task_id='transform_rs_game_genre', python_callable=transform_rs_game_genre, provide_context=True)
    task_transform_rs_game_store = PythonOperator(task_id='transform_rs_game_store', python_callable=transform_rs_game_store, provide_context=True)
    task_transform_rs_game_rating = PythonOperator(task_id='transform_rs_game_rating', python_callable=transform_rs_game_rating, provide_context=True)
    task_transform_rs_game_tag = PythonOperator(task_id='transform_rs_game_tag', python_callable=transform_rs_game_tag, provide_context=True)
    task_transform_rs_game_publisher = PythonOperator(task_id='transform_rs_game_publisher', python_callable=transform_rs_game_publisher, provide_context=True)


    # Dependencies Configuration
    # =========================================================================
    task_set_data_directory >> task_extract_new_game_list >> task_extract_game_detail >> task_check_new_publisher >> task_extract_publisher

    task_extract_publisher >> task_check_new_genre >> task_extract_genre >> task_transform_entity_genre
    task_extract_publisher >> task_check_new_tag >> task_extract_tag >> task_transform_entity_tag
    task_extract_publisher >> task_check_new_store >> task_extract_store >> task_transform_entity_store
    task_extract_publisher >> task_check_new_platform >> task_extract_platform >> task_transform_entity_platform

    task_extract_publisher >> task_transform_entity_publisher
    task_extract_publisher >> task_transform_entity_game
    task_extract_publisher >> task_transform_entity_rating
    task_extract_publisher >> task_transform_rs_game_platform
    task_extract_publisher >> task_transform_rs_game_genre
    task_extract_publisher >> task_transform_rs_game_store
    task_extract_publisher >> task_transform_rs_game_rating
    task_extract_publisher >> task_transform_rs_game_tag
    task_extract_publisher >> task_transform_rs_game_publisher


    	# ------------------------------------------------------------------------------------------------------[DEPENDENCIES FOR SKIPPING EXTRACTION]
#     task_set_data_directory >> task_check_new_publisher >> task_extract_publisher
#     task_set_data_directory >> task_check_new_genre
#     task_set_data_directory >> task_check_new_tag
#     task_set_data_directory >> task_check_new_store
#     task_set_data_directory >> task_check_new_platform

#     task_set_data_directory >> task_transform_entity_publisher
#     task_set_data_directory >> task_transform_entity_game
#     task_set_data_directory >> task_transform_entity_rating
#     task_set_data_directory >> task_transform_rs_game_platform
#     task_set_data_directory >> task_transform_rs_game_genre
#     task_set_data_directory >> task_transform_rs_game_store
#     task_set_data_directory >> task_transform_rs_game_rating
#     task_set_data_directory >> task_transform_rs_game_tag
#     task_set_data_directory >> task_transform_rs_game_publisher

#     task_set_data_directory >> task_check_new_genre >> task_extract_genre >> task_transform_entity_genre
#     task_set_data_directory >> task_check_new_tag >> task_extract_tag >> task_transform_entity_tag
#     task_set_data_directory >> task_check_new_store >> task_extract_store >> task_transform_entity_store
#     task_set_data_directory >> task_check_new_platform >> task_extract_platform >> task_transform_entity_platform
    
    


    # Extract Updated Game Details --------------------------------------------------------------------------------------------------[DRAFT: TO INTEGRATE]
    # ========================================================== Updated Game Info: Not working it for now [MAYBE SPLIT IT INTO ANOTHER DAG]
    # task_extract_updates_game_list = PythonOperator(
    #     task_id="extract_updated_game_list",
    #     python_callable=extract_game_list,
    #     provide_context=True,
    #     op_kwargs={
    #         'extraction_task': 'extract_updates',
    #         'year': datetime.today().year,
    #         'month': datetime.today().month
    #     }
    # )