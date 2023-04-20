# ------------------------- Monthly New Games ETL -------------------------

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
from load import *
from database import *

# Load Python Operators
################################################################################################

import os
import sys
dags_dir = os.path.dirname(os.path.abspath(__file__))
airflow_dir = os.path.dirname(dags_dir)
root_dir = os.path.dirname(airflow_dir)
sys.path.append(root_dir)

from extract import *

# Switch [Whether to perform Extraction]
################################################################################################

# if False --> GameList, GameDetails and Publisher will not be extracted
perform_extraction = True


# Set local machine data directory (Directory used to store back-up data)
################################################################################################

def set_new_games_data_directory(**kwargs):
    ti = kwargs["ti"]

    # check if data directory exit, if not, create the folder
    if "data" not in os.listdir(root_dir):
        os.mkdir(os.path.join(root_dir, "data"))

    root_data_directory = os.path.join(root_dir, "data")

    # check if monthly_new_games directory exist, if not, create the folder
    if "monthly_new_games" not in os.listdir(root_data_directory):
        os.mkdir(os.path.join(root_data_directory, "monthly_new_games"))
        os.mkdir(os.path.join(root_data_directory, "monthly_new_games", "raw_data"))
        os.mkdir(os.path.join(root_data_directory, "monthly_new_games", "transformed_data"))
    
    # push the directory to descendent tasks
    data_directory = os.path.join(root_data_directory, "monthly_new_games")
    ti.xcom_push("root_data_directory", data_directory)


# Create DAG Object
################################################################################################

with DAG(
    dag_id='monthly_new_games',
        schedule_interval='@monthly',
        start_date=datetime(2023, 2, 1),
        catchup=True
) as dag:
    # Set New Games Data Directory
    task_set_data_directory = PythonOperator(
    task_id="set_data_directory",
    python_callable=set_new_games_data_directory,
    provide_context=True
    )

    # Extract Games and Publishers
    # =========================================================================
    if perform_extraction:
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

        # Extract New Publisher
        task_extract_publisher = PythonOperator(
            task_id="extract_new_publisher",
            python_callable=extract_publisher,
            provide_context=True
        )
        
    
        # Check for new games (only new games will be processed in this pipeline)
        task_check_new_game = PythonOperator(
            task_id="check_new_game",
            python_callable=check_games,
            provide_context=True,
            op_kwargs={'check_for_new': True}
        )

        # Check New Publisher (from Game Detail)
        task_check_new_publisher = PythonOperator(
            task_id="check_new_publisher",
            python_callable=check_new_records,
            provide_context=True,
            op_kwargs={
                'entity': 'publisher',
                'file_name': 'game_details_publisher.csv'
            }
        )


    # Check for New Information (Genre, Tags etc.)
    # =========================================================================

    # Check New Genre (from Game List)
    task_check_new_genre = PythonOperator(
        task_id="check_new_genre",
        python_callable=check_new_records,
        provide_context=True,
        op_kwargs={
        'entity': 'genre',
        'file_name': 'game_genre.csv'
        }
    )

    # Check New Tag (from Game List)
    task_check_new_tag = PythonOperator(
        task_id="check_new_tag",
        python_callable=check_new_records,
        provide_context=True,
        op_kwargs={
        'entity': 'tag',
        'file_name': 'game_tag.csv'
        }
    )

    # Check New Store (from Game List)
    task_check_new_store = PythonOperator(
        task_id="check_new_store",
        python_callable=check_new_records,
        provide_context=True,
        op_kwargs={
        'entity': 'store',
        'file_name': 'game_store.csv'
        }
    )

    # Check New Platform (from Game List)
    task_check_new_platform = PythonOperator(
        task_id="check_new_platform",
        python_callable=check_new_records,
        provide_context=True,
        op_kwargs={
        'entity': 'platform',
        'file_name': 'game_platform.csv'
        }
    )

    # Check New Rating (transformed into transformed_data folder, from Game-Rating)
    task_check_new_rating = PythonOperator(
        task_id="check_new_rating",
        python_callable=check_new_rating,
        provide_context=True
    )


    # Extract New Information (Genre, Tags etc.)
    # =========================================================================

    # Extract New Genre
    task_extract_genre = PythonOperator(
        task_id="extract_new_genre",
        python_callable=extract_genre,
        provide_context=True
    )

    # Extract New Tag
    task_extract_tag = PythonOperator(
        task_id="extract_new_tag",
        python_callable=extract_tag,
        provide_context=True
    )

    # Extract New Store
    task_extract_store = PythonOperator(
        task_id="extract_new_store",
        python_callable=extract_store,
        provide_context=True
    )

    # Extract New Parent Platform
    task_extract_parent_platform = PythonOperator(
        task_id="extract_new_parent_platform",
        python_callable=extract_parent_platform,
        provide_context=True
    )

    # Extract New Platform
    task_extract_platform = PythonOperator(
        task_id="extract_new_platform",
        python_callable=extract_platform,
        provide_context=True
    )


    # Transform New Information (Genre, Tags etc.)
    # =========================================================================
    task_transform_entity_game = PythonOperator(
        task_id='transform_entity_game', 
        python_callable=transform_entity_game, 
        provide_context=True
    )
    
    task_transform_entity_publisher = PythonOperator(
        task_id='transform_entity_publisher', 
        python_callable=transform_entity_publisher, 
        provide_context=True
    )
    
    task_transform_entity_genre = PythonOperator(
        task_id='transform_entity_genre', 
        python_callable=transform_entity_genre, 
        provide_context=True
    )
    
    task_transform_entity_tag = PythonOperator(
        task_id='transform_entity_tag', 
        python_callable=transform_entity_tag, 
        provide_context=True
    )
    
    task_transform_entity_store = PythonOperator(
        task_id='transform_entity_store', 
        python_callable=transform_entity_store, 
        provide_context=True
    )
    
    task_transform_entity_parent_platform = PythonOperator(
        task_id='transform_entity_parent_platform', 
        python_callable=transform_entity_parent_platform, 
        provide_context=True
    )
    
    task_transform_entity_platform = PythonOperator(
        task_id='transform_entity_platform', 
        python_callable=transform_entity_platform, 
        provide_context=True
    )
    
    task_transform_entity_rating = PythonOperator(
        task_id='transform_entity_rating',
        python_callable=transform_entity_rating, 
        provide_context=True
    )
    
    task_transform_rs_game_platform = PythonOperator(
        task_id='transform_rs_game_platform', 
        python_callable=transform_rs_game_platform, 
        provide_context=True
    )
    
    task_transform_rs_game_genre = PythonOperator(
        task_id='transform_rs_game_genre', 
        python_callable=transform_rs_game_genre, 
        provide_context=True
    )
    
    task_transform_rs_game_store = PythonOperator(
        task_id='transform_rs_game_store', 
        python_callable=transform_rs_game_store, 
        provide_context=True
    )
    
    task_transform_rs_game_rating = PythonOperator(
        task_id='transform_rs_game_rating', 
        python_callable=transform_rs_game_rating, 
        provide_context=True
    )
    
    task_transform_rs_game_tag = PythonOperator(
        task_id='transform_rs_game_tag', 
        python_callable=transform_rs_game_tag, 
        provide_context=True
    )
    
    task_transform_rs_game_publisher = PythonOperator(
        task_id='transform_rs_game_publisher', 
        python_callable=transform_rs_game_publisher, 
        provide_context=True
    )

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
    if perform_extraction:
        # Dependent on GameList --> GameDetail --> Publisher path
        task_set_data_directory >> task_extract_new_game_list >> task_check_new_game >> task_extract_game_detail >> task_check_new_publisher >> task_extract_publisher
        
        task_extract_publisher >> task_check_new_genre >> task_extract_genre >> task_transform_entity_genre
        task_extract_publisher >> task_check_new_tag >> task_extract_tag >> task_transform_entity_tag
        task_extract_publisher >> task_check_new_store >> task_extract_store >> task_transform_entity_store
        # Platform handling is below
        task_extract_publisher >> task_check_new_platform

        # Independent of GameList --> GameDetail --> Publisher path 
        task_extract_publisher >> task_transform_entity_game
        task_extract_publisher >> task_transform_entity_publisher
        task_extract_publisher >> task_transform_entity_rating       
        task_extract_publisher >> task_transform_rs_game_platform
        task_extract_publisher >> task_transform_rs_game_genre
        task_extract_publisher >> task_transform_rs_game_store
        task_extract_publisher >> task_transform_rs_game_rating
        task_extract_publisher >> task_transform_rs_game_tag
        task_extract_publisher >> task_transform_rs_game_publisher
    else:
        task_set_data_directory >> task_check_new_genre
        task_set_data_directory >> task_check_new_tag
        task_set_data_directory >> task_check_new_store
        task_set_data_directory >> task_check_new_platform

        task_set_data_directory >> task_transform_entity_publisher
        task_set_data_directory >> task_transform_entity_game
        task_set_data_directory >> task_transform_entity_rating
        task_set_data_directory >> task_transform_rs_game_platform
        task_set_data_directory >> task_transform_rs_game_genre
        task_set_data_directory >> task_transform_rs_game_store
        task_set_data_directory >> task_transform_rs_game_rating
        task_set_data_directory >> task_transform_rs_game_tag
        task_set_data_directory >> task_transform_rs_game_publisher

        task_set_data_directory >> task_check_new_genre >> task_extract_genre >> task_transform_entity_genre
        task_set_data_directory >> task_check_new_tag >> task_extract_tag >> task_transform_entity_tag
        task_set_data_directory >> task_check_new_store >> task_extract_store >> task_transform_entity_store

    
    # Loading Entity
    task_transform_entity_game >> task_load_entity_game
    task_transform_entity_publisher >> task_load_entity_publisher
    task_transform_entity_genre >> task_load_entity_genre
    task_transform_entity_tag >> task_load_entity_tag
    task_transform_entity_store >> task_load_entity_store
    task_transform_entity_rating >> task_check_new_rating >> task_load_entity_rating

    # Platform Handling
    task_check_new_platform >> task_extract_parent_platform
    task_check_new_platform >> task_extract_platform
    task_extract_parent_platform >> task_transform_entity_parent_platform
    task_extract_platform >> task_transform_entity_parent_platform
    task_transform_entity_parent_platform >> task_transform_entity_platform
    task_transform_entity_platform >> task_load_entity_parent_platform >> task_load_entity_platform

    # Loading Relationship
    task_load_entity_game >> task_load_game_publisher
    task_load_entity_game >> task_load_game_genre
    task_load_entity_game >> task_load_game_tag
    task_load_entity_game >> task_load_game_store
    task_load_entity_game >> task_load_game_rating
    task_load_entity_game >> task_load_game_platform

    task_load_entity_publisher >> task_load_game_publisher
    task_load_entity_genre >> task_load_game_genre
    task_load_entity_tag >> task_load_game_tag
    task_load_entity_store >> task_load_game_store
    task_load_entity_rating >> task_load_game_rating
    task_load_entity_platform >> task_load_game_platform

    task_transform_rs_game_publisher >> task_load_game_publisher
    task_transform_rs_game_genre >> task_load_game_genre
    task_transform_rs_game_tag >> task_load_game_tag
    task_transform_rs_game_store >> task_load_game_store
    task_transform_rs_game_rating >> task_load_game_rating
    task_transform_rs_game_platform >> task_load_game_platform