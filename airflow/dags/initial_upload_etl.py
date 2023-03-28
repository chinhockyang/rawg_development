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
from load import *
from database import *

# Switch [Whether to perform Extraction]
################################################################################################

# ------------------------------------------------------------------[FOR DEVELOPMENT PURPOSE]
perform_extraction = False


# Set local machine data directory (Directory used to store back-up data)
################################################################################################

def set_data_directory(**kwargs):
    ti = kwargs["ti"]

    # check if data directory exit, if not, create the folder
    if "data" not in os.listdir(root_dir):
        os.mkdir(os.path.join(root_dir, "data"))

    root_data_directory = os.path.join(root_dir, "data")

    # check if initial_upload directory exist, if not, create the folder
    if "initial_upload" not in os.listdir(root_data_directory):
        os.mkdir(os.path.join(root_data_directory, "initial_upload"))
        os.mkdir(os.path.join(root_data_directory, "initial_upload", "raw_data"))
        os.mkdir(os.path.join(root_data_directory, "initial_upload", "transformed_data"))
    

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
    # Setting up
    # =========================================================================

    task_set_data_directory = PythonOperator(
        task_id="set_data_directory",
        python_callable=set_data_directory,
        provide_context=True
    )

    task_create_database_tables = PythonOperator(
        task_id="create_database_tables",
        python_callable=create_database_tables
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
    # Entities:
    # -------------------
    
    # game
    task_load_entity_game = PythonOperator(
            task_id='load_entity_game', 
            python_callable=load_entity_data, 
            provide_context=True,
            op_kwargs={'table': Game, 'file_name': 'entity_game.csv', 'monthly_update': False},
    )

    # publisher
    task_load_entity_publisher = PythonOperator(
            task_id='load_entity_publisher', 
            python_callable=load_entity_data, 
            provide_context=True,
            op_kwargs={'table': Publisher, 'file_name': 'entity_publisher.csv', 'monthly_update': False},
    )
    
    # genre
    task_load_entity_genre = PythonOperator(
            task_id='load_entity_genre', 
            python_callable=load_entity_data, 
            provide_context=True,
            op_kwargs={'table': Genre, 'file_name': 'entity_genre.csv', 'monthly_update': False},
    )

    # tag
    task_load_entity_tag = PythonOperator(
            task_id='load_entity_tag', 
            python_callable=load_entity_data, 
            provide_context=True,
            op_kwargs={'table': Tag, 'file_name': 'entity_tag.csv', 'monthly_update': False},
    )

    # store
    task_load_entity_store = PythonOperator(
            task_id='load_entity_store', 
            python_callable=load_entity_data, 
            provide_context=True,
            op_kwargs={'table': Store, 'file_name': 'entity_store.csv', 'monthly_update': False},
    )

    # parent platform
    task_load_entity_parent_platform = PythonOperator(
            task_id='load_entity_parent_platform', 
            python_callable=load_entity_data, 
            provide_context=True,
            op_kwargs={'table': ParentPlatform, 'file_name': 'entity_parent_platform.csv', 'monthly_update': False},
    )

    # platform
    task_load_entity_platform = PythonOperator(
            task_id='load_entity_platform', 
            python_callable=load_entity_data, 
            provide_context=True,
            op_kwargs={'table': Platform, 'file_name': 'entity_platform.csv', 'monthly_update': False},
    )

    # rating
    task_load_entity_rating = PythonOperator(
            task_id='load_entity_rating', 
            python_callable=load_entity_data, 
            provide_context=True,
            op_kwargs={'table': Rating, 'file_name': 'entity_rating.csv', 'monthly_update': False},
    )

    # Relationship
    # -------------------
    # GamePublisher
    task_load_game_publisher = PythonOperator(
            task_id='load_rs_game_publisher', 
            python_callable=load_game_relationship_data, 
            provide_context=True,
            op_kwargs={'entity': 'publisher', 'table': GamePublisher, 'file_name': 'rs_game_publisher.csv', 'monthly_update': False},
    )

    # GameGenre
    task_load_game_genre = PythonOperator(
            task_id='load_rs_game_genre', 
            python_callable=load_game_relationship_data, 
            provide_context=True,
            op_kwargs={'entity': 'genre', 'table': GameGenre, 'file_name': 'rs_game_genre.csv', 'monthly_update': False},
    )

    # GameTag
    task_load_game_tag = PythonOperator(
            task_id='load_rs_game_tag', 
            python_callable=load_game_relationship_data, 
            provide_context=True,
            op_kwargs={'entity': 'tag', 'table': GameTag, 'file_name': 'rs_game_tag.csv', 'monthly_update': False},
    )

    # GameStore
    task_load_game_store = PythonOperator(
            task_id='load_rs_game_store', 
            python_callable=load_game_relationship_data, 
            provide_context=True,
            op_kwargs={'entity': 'store', 'table': GameStore, 'file_name': 'rs_game_store.csv', 'monthly_update': False},
    )

    # GamePlatform
    task_load_game_platform = PythonOperator(
            task_id='load_rs_game_platform', 
            python_callable=load_game_relationship_data, 
            provide_context=True,
            op_kwargs={'entity': 'platform', 'table': GamePlatform, 'file_name': 'rs_game_platform.csv', 'monthly_update': False},
    )

    # GameRating
    task_load_game_rating = PythonOperator(
            task_id='load_rs_game_rating', 
            python_callable=load_game_relationship_data, 
            provide_context=True,
            op_kwargs={'entity': 'rating', 'table': GameRating, 'file_name': 'rs_game_rating.csv', 'monthly_update': False},
    )


    # Dependencies Configuration
    # =========================================================================
    task_create_database_tables >> task_set_data_directory

    if perform_extraction:
        # Dependent on GameList --> GameDetail --> Publisher path
        task_set_data_directory >> task_extract_game_list >> task_extract_game_detail >> task_extract_publisher
        task_extract_publisher >> task_transform_entity_game
        task_extract_publisher >> task_transform_entity_rating
        task_extract_publisher >> task_transform_rs_game_platform
        task_extract_publisher >> task_transform_rs_game_genre
        task_extract_publisher >> task_transform_rs_game_store
        task_extract_publisher >> task_transform_rs_game_rating
        task_extract_publisher >> task_transform_rs_game_tag
        task_extract_publisher >> task_transform_rs_game_publisher

        # Independent of GameList --> GameDetail --> Publisher path
        task_set_data_directory >> task_extract_parent_platform >> task_transform_entity_parent_platform
        task_set_data_directory >> task_extract_platform >> task_transform_entity_platform
        task_set_data_directory >> task_extract_publisher >> task_transform_entity_publisher
        task_set_data_directory >> task_extract_tag >> task_transform_entity_tag
        task_set_data_directory >> task_extract_genre >> task_transform_entity_genre
        task_set_data_directory >> task_extract_store >> task_transform_entity_store

    else:
        task_set_data_directory >> task_transform_entity_game
        task_set_data_directory >> task_transform_entity_rating
        task_set_data_directory >> task_transform_rs_game_platform
        task_set_data_directory >> task_transform_rs_game_genre
        task_set_data_directory >> task_transform_rs_game_store
        task_set_data_directory >> task_transform_rs_game_rating
        task_set_data_directory >> task_transform_rs_game_tag
        task_set_data_directory >> task_transform_rs_game_publisher
        task_set_data_directory >> task_transform_entity_parent_platform
        task_set_data_directory >> task_transform_entity_platform
        task_set_data_directory >> task_transform_entity_publisher
        task_set_data_directory >> task_transform_entity_tag
        task_set_data_directory >> task_transform_entity_genre
        task_set_data_directory >> task_transform_entity_store
    
    # Loading Entity
    task_transform_entity_game >> task_load_entity_game
    task_transform_entity_publisher >> task_load_entity_publisher
    task_transform_entity_genre >> task_load_entity_genre
    task_transform_entity_tag >> task_load_entity_tag
    task_transform_entity_store >> task_load_entity_store
    task_transform_entity_parent_platform >> task_load_entity_parent_platform >> task_load_entity_platform
    task_transform_entity_rating >> task_load_entity_rating

    # Loading Relationship
    task_load_entity_game >> task_load_game_publisher
    task_load_entity_game >> task_load_game_genre
    task_load_entity_game >> task_load_game_tag
    task_load_entity_game >> task_load_game_store
    task_load_entity_game >> task_load_game_platform
    task_load_entity_game >> task_load_game_rating

    task_load_entity_publisher >> task_load_game_publisher
    task_load_entity_genre >> task_load_game_genre
    task_load_entity_tag >> task_load_game_tag
    task_load_entity_store >> task_load_game_store
    task_load_entity_platform >> task_load_game_platform
    task_load_entity_rating >> task_load_game_rating

    task_transform_rs_game_publisher >> task_load_game_publisher
    task_transform_rs_game_genre >> task_load_game_genre
    task_transform_rs_game_tag >> task_load_game_tag
    task_transform_rs_game_store >> task_load_game_store
    task_transform_rs_game_platform >> task_load_game_platform
    task_transform_rs_game_rating >> task_load_game_rating
