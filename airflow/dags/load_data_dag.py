
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

from database import Game
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
    ]

    # Drop All Tables
    Base.metadata.drop_all(engine, table_objects)

    # Create All Tables
    Base.metadata.create_all(engine, table_objects)

    data_path = os.path.join(data_directory, "entity_game.csv")
    df_data = pd.read_csv(data_path)

    game_obj = convert_df_to_lst_of_table_objects(df_data, Game)

    session.add_all(game_obj)
    session.commit()
    session.close()

    if len(game_obj) > 0:
        output_str = f"{len(game_obj)} objects uploaded into database."
    else:
        output_str = "Upload Failure"

    return output_str


with DAG(
    dag_id='rawg_db_test',
    schedule_interval='@monthly',
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