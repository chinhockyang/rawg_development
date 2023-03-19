
# Just a DAG to test out Airflow ETL
# ======================================================

import os
import sys
dags_dir = os.path.dirname(os.path.abspath(__file__))
airflow_dir = os.path.dirname(dags_dir)
root_dir = os.path.dirname(airflow_dir)
sys.path.append(root_dir)

import pandas as pd
from datetime import datetime

from airflow.models import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash_operator import BashOperator

from database import *
from utilities import session_engine_from_connection_string, convert_df_to_lst_of_table_objects
from database import Base

from dotenv import load_dotenv
dotenv = load_dotenv()
CONNECTION_STRING = os.getenv("MYSQL_CONNECTION_STRING")

data_directory = os.path.join(os.getcwd(), "transformed_data")


def get_dataset():
    data_path = os.path.join(data_directory, "entity_game.csv")
    df_data = pd.read_csv(data_path)

    if len(df_data) > 0:
        output_str = f"Data loaded successfully. Data has {len(df_data)} rows."
    else:
        output_str = "No Data is loaded"
    return str(output_str)


def load_dataset():
    # create session and engine
    session, engine = session_engine_from_connection_string(CONNECTION_STRING)

    conn = engine.connect()

    # tables to be created
    table_objects = [
        Game.__table__,
        Publisher.__table__,
        Genre.__table__,
        Tag.__table__,
        Store.__table__,
        Platform.__table__,
        ParentPlatform.__table__,
        Rating.__table__,
        
        GamePublisher.__table__,
        GameGenre.__table__,
        GameTag.__table__,
        GameStore.__table__,
        GamePlatform.__table__,
        # Platform_ParentPlatform.__table__,
        GameRating.__table__,
    ]

    # Drop All Tables
    Base.metadata.drop_all(engine, table_objects)

    # Create All Tables
    Base.metadata.create_all(engine, table_objects)

    # Game Entity                                                       # TODO: Wrap into reusable function
    game_data_path = os.path.join(data_directory, "entity_game.csv")
    df_game_data = pd.read_csv(game_data_path)
    game_obj = convert_df_to_lst_of_table_objects(df_game_data, Game)
    if len(game_obj) > 0:
        print(f"{len(game_obj)} Game objects uploaded into database.")
    else:
        print("Upload Game Failed")

    # Publisher Entity
    publisher_data_path = os.path.join(data_directory, "entity_publisher.csv")
    df_publisher_data = pd.read_csv(publisher_data_path)
    publisher_obj = convert_df_to_lst_of_table_objects(df_publisher_data, Publisher)
    if len(publisher_obj) > 0:
        print(f"{len(publisher_obj)} Publisher objects uploaded into database.")
    else:
        print("Upload Publisher Failed")

    # Genre Entity
    genre_data_path = os.path.join(data_directory, "entity_genre.csv")
    df_genre_data = pd.read_csv(genre_data_path)
    genre_obj = convert_df_to_lst_of_table_objects(df_genre_data, Genre)
    if len(genre_obj) > 0:
        print(f"{len(genre_obj)} Genre objects uploaded into database.")
    else:
        print("Upload Genre Failed")

    # Tag Entity
    tag_data_path = os.path.join(data_directory, "entity_tag.csv")
    df_tag_data = pd.read_csv(tag_data_path)
    tag_obj = convert_df_to_lst_of_table_objects(df_tag_data, Tag)
    if len(tag_obj) > 0:
        print(f"{len(tag_obj)} Tag objects uploaded into database.")
    else:
        print("Upload Tag Failed")

    # Store Entity
    store_data_path = os.path.join(data_directory, "entity_store.csv")
    df_store_data = pd.read_csv(store_data_path)
    store_obj = convert_df_to_lst_of_table_objects(df_store_data, Store)
    if len(store_obj) > 0:
        print(f"{len(store_obj)} Store objects uploaded into database.")
    else:
        print("Upload Store Failed")

    # Platform Entity
    platform_data_path = os.path.join(data_directory, "entity_platform.csv")
    df_platform_data = pd.read_csv(platform_data_path)
    platform_obj = convert_df_to_lst_of_table_objects(df_platform_data, Platform)
    if len(platform_obj) > 0:
        print(f"{len(platform_obj)} Platform objects uploaded into database.")
    else:
        print("Upload Platform Failed")

    # Parent Platform Entity
    parent_platform_data_path = os.path.join(data_directory, "entity_parent_platform.csv")
    df_parent_platform_data = pd.read_csv(parent_platform_data_path)
    parent_platform_obj = convert_df_to_lst_of_table_objects(df_parent_platform_data, ParentPlatform)
    if len(parent_platform_obj) > 0:
        print(f"{len(parent_platform_obj)} Parent Platform objects uploaded into database.")
    else:
        print("Upload Parent Platform Failed")

    # Rating
    rating_data_path = os.path.join(data_directory, "entity_rating.csv")
    df_rating_data = pd.read_csv(rating_data_path)
    rating_obj = convert_df_to_lst_of_table_objects(df_rating_data, Rating)
    if len(rating_obj) > 0:
        print(f"{len(rating_obj)} Rating objects uploaded into database.")
    else:
        print("Upload Rating Failed")

    # GamePublisher
    game_publisher_data_path = os.path.join(data_directory, "rs_game_publisher.csv")
    df_game_publisher_data = pd.read_csv(game_publisher_data_path)
    game_publisher_obj = convert_df_to_lst_of_table_objects(df_game_publisher_data, GamePublisher)


    # GameGenre
    game_genre_data_path = os.path.join(data_directory, "rs_game_genre.csv")
    df_game_genre_data = pd.read_csv(game_genre_data_path)
    game_genre_obj = convert_df_to_lst_of_table_objects(df_game_genre_data, GameGenre)
    

    # GameTag
    game_tag_data_path = os.path.join(data_directory, "rs_game_tag.csv")
    df_game_tag_data = pd.read_csv(game_tag_data_path)
    game_tag_obj = convert_df_to_lst_of_table_objects(df_game_tag_data, GameTag)
    

    # GameStore
    game_store_data_path = os.path.join(data_directory, "rs_game_store.csv")
    df_game_store_data = pd.read_csv(game_store_data_path)
    game_store_obj = convert_df_to_lst_of_table_objects(df_game_store_data, GameStore)
    

    # GamePlatform
    game_platform_data_path = os.path.join(data_directory, "rs_game_platform.csv")
    df_game_platform_data = pd.read_csv(game_platform_data_path)
    game_platform_obj = convert_df_to_lst_of_table_objects(df_game_platform_data, GamePlatform)
    

    # Platform_Parent_platform
    # platform_parent_platform_data_path = os.path.join(data_directory, "rs_platform_parent_platform.csv")
    # df_platform_parent_platform_data = pd.read_csv(platform_parent_platform_data_path)
    # platform_parent_platform_obj = convert_df_to_lst_of_table_objects(df_platform_parent_platform_data, Platform_ParentPlatform)
    

    # GameRating
    game_rating_data_path = os.path.join(data_directory, "rs_game_rating.csv")
    df_game_rating_data = pd.read_csv(game_rating_data_path)
    game_rating_obj = convert_df_to_lst_of_table_objects(df_game_rating_data, GameRating)


    session.add_all(game_obj)
    session.commit()

    session.add_all(publisher_obj)
    session.commit()

    session.add_all(genre_obj)
    session.commit()

    session.add_all(tag_obj)
    session.commit()

    session.add_all(store_obj)
    session.commit()

    session.add_all(parent_platform_obj)
    session.commit()

    session.add_all(platform_obj)
    session.commit()

    session.add_all(rating_obj)
    session.commit()

    session.add_all(game_publisher_obj)
    session.commit()

    session.add_all(game_genre_obj)
    session.commit()

    session.add_all(game_tag_obj)
    session.commit()

    session.add_all(game_store_obj)
    session.commit()

    session.add_all(game_platform_obj)
    session.commit()

    session.add_all(game_rating_obj)
    session.commit()



with DAG(
    dag_id='rawg_db_test',
    schedule_interval=None,
    start_date=datetime(year=2023,month=2,day=28),
    catchup=False
) as dag:
    # 1. Get Data from local csv folder to upload
    task_get_dataset = PythonOperator(
        task_id='get_dataset',
        python_callable=get_dataset,
        do_xcom_push=True
    )

    # 2. Upload Data into Database
    task_load_data = PythonOperator(
        task_id="load_data",
        python_callable=load_dataset,
        do_xcom_push=True
    )    

    task_get_dataset >> task_load_data