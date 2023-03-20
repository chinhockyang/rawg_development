from datetime import datetime
from airflow import DAG
from airflow.operators.python import PythonOperator
from transform import *


default_args = {
    'owner': 'airflow',
}
with DAG(
    'transform_data_dag',
    default_args=default_args,
    description='IS3107_Project',
    schedule_interval=None,
    start_date=datetime(2021, 1, 1),
    catchup=False,
    tags=['example'],
) as dag:
    transform_entity_game_task = PythonOperator(task_id='transform_entity_game_task', python_callable=transform_entity_game)
    transform_entity_parent_platform_task = PythonOperator(task_id='transform_entity_parent_platform_task', python_callable=transform_entity_parent_platform)
    transform_entity_platform_task = PythonOperator(task_id='transform_entity_platform_task', python_callable=transform_entity_platform)
    transform_entity_publisher_task = PythonOperator(task_id='transform_entity_publisher_task', python_callable=transform_entity_publisher)
    transform_entity_tag_task = PythonOperator(task_id='transform_entity_tag_task', python_callable=transform_entity_tag)
    transform_entity_genre_task = PythonOperator(task_id='transform_entity_genre_task', python_callable=transform_entity_genre)
    transform_entity_store_task = PythonOperator(task_id='transform_entity_store_task', python_callable=transform_entity_store)
    transform_entity_rating_task = PythonOperator(task_id='transform_entity_rating_task', python_callable=transform_entity_rating)
    transform_rs_game_platform_task = PythonOperator(task_id='transform_rs_game_platform_task', python_callable=transform_rs_game_platform)
    transform_rs_game_genre_task = PythonOperator(task_id='transform_rs_game_genre_task', python_callable=transform_rs_game_genre)
    transform_rs_game_store_task = PythonOperator(task_id='transform_rs_game_store_task', python_callable=transform_rs_game_store)
    transform_rs_game_rating_task = PythonOperator(task_id='transform_rs_game_rating_task', python_callable=transform_rs_game_rating)
    transform_rs_game_tag_task = PythonOperator(task_id='transform_rs_game_tag_task', python_callable=transform_rs_game_tag)
    transform_rs_game_publisher_task = PythonOperator(task_id='transform_rs_game_publisher_task', python_callable=transform_rs_game_publisher)

    transform_entity_game
    transform_entity_parent_platform
    transform_entity_platform
    transform_entity_publisher
    transform_entity_tag
    transform_entity_genre
    transform_entity_store
    transform_entity_rating
    transform_rs_game_platform
    transform_rs_game_genre
    transform_rs_game_store
    transform_rs_game_rating
    transform_rs_game_tag
    transform_rs_game_publisher