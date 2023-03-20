
# ------------------------- Extract Functions for ETL -------------------------

# Import Libraries and Transform Functions
import pandas as pd
import os

import requests
from datetime import datetime

import sys
root_dir = os.path.dirname(os.path.abspath(__file__))
sys.path.append(root_dir)
from transform import (
    transform_game_list_api,
    transform_game_detail_api,
    create_dictionary_from_df
)

# API Configurations and Token [Global Variables]
################################################################################################

from dotenv import load_dotenv
load_dotenv()

RAWG_TOKEN = os.getenv('RAWG_TOKEN')

data_directory = os.path.join(os.getcwd(), "raw_data")


# Functions Used to Perform API Request
################################################################################################

# -------------------------------------------------------------------------[TODO]: Create Try and Catch to re-request if Error 502

def get_list_response(API_KEY: str, endpoint: str, **kwargs):
    """
    Function to extract data from RAWG's List APIs (e.g. Game list, Developer List).
    Return a json object on successful extraction (status_code: 200)
    Return a response object on failure of extraction

    Parameters
    ----------
    API_KEY: str
        API Token

    endpoint: str
        The end-point of the API request to be sent (e.g. "games" or "platforms/lists/parents")

    **kwargs
        API request payload (specify parameter and value)

    """

    url = f"https://api.rawg.io/api/{endpoint}"

    headers = {
        'accept': 'application/json'
    }

    params ={
        'key': API_KEY
    }

    for k,v in kwargs.items():
        params[k] = v
    
    response = requests.get(url, headers=headers, params=params)
    
    if response.status_code == 200:
        resp_json = response.json()
        return resp_json
    else:
        print(f"Error extracting data - Error Code {response.status_code}")
        return response


def get_detail_response(API_KEY, endpoint, id, game_info=False, **kwargs):
    """
    Function to extract data from RAWG's Detailed APIs (e.g. Game Details).
    Return a json object on successful extraction (status_code: 200)
    Return a response object on failure of extraction

    Parameters
    ----------
    API_KEY: str
        API Token

    endpoint: str
        The end-point of the API request to be sent (e.g. "games", "developers" etc)

    id: 
        The ID (index or slug) of the item to be retrieved
    
    games_info: 
        If accessing specific information of the Games API (e.g. achievements)
    
    **kwargs
        API request payload (specify parameter and value)

    """
    if game_info:
        url = f"https://api.rawg.io/api/games/{id}/{endpoint}"
    else:
        url = f"https://api.rawg.io/api/{endpoint}/{id}"

    headers = {
        'accept': 'application/json'
    }

    params = {
        'key': API_KEY
    }

    for k,v in kwargs.items():
        params[k] = v
    
    response = requests.get(url, headers=headers, params=params)
    
    if response.status_code == 200:
        resp_json = response.json()
        return resp_json
    else:
        print(url)
        print(f"Error extracting data - Error Code {response.status_code}")
        return response
    

# Python Functions of Python Operators
################################################################################################

def extract_game_list(**kwargs):
    # Task Instance
    ti = kwargs["ti"]

    # type of extraction task
    extraction_task = kwargs["extraction_task"]

    # Current Year
    if "year" in kwargs:
        year = kwargs["year"]

    # Current Month
    if "month" in kwargs:
        month = kwargs["month"]
    

    if extraction_task == "initial_upload":
        # Data Date Range from 2018-01-01 to 2023-01-01
        # ----------------------------------------------- COMMENT OUT TO NOT OVER-REQUEST
        date_range = [
            # "2018-01-01,2018-06-30",
            # "2018-07-01,2018-12-31",
            # "2019-01-01,2019-06-30",
            # "2019-07-01,2019-12-31",
            # "2020-01-01,2020-06-30",
            # "2020-07-01,2020-12-31",
            # "2021-01-01,2021-06-30",
            # "2021-07-01,2021-12-31",
            # "2022-01-01,2022-06-30",
            # "2022-07-01,2022-12-31",
            # "2023-01-01,2023-01-31"
            "2023-01-01,2023-01-03"
        ]
    elif extraction_task == "extract_new_games":
        if month == 1:
            month_start = 12
            year_start = year - 1
        
        else:
            month_start = month - 1
            year_start = year
    
        start_date = datetime(year_start, month_start, 1)
        end_date = datetime(year, month, 1)
        
        date_range = [
            f"{start_date.strftime('%Y-%m-%d')},{end_date.strftime('%Y-%m-%d')}"
        ]

    elif extraction_task == "extract_updates":
        if month == 1:
            update_month_start = 12
            update_year_start = year - 1
        else:
            update_month_start = month - 1
            update_year_start = year

        start_date = datetime(update_year_start, update_month_start, 1)
        end_date = datetime(year, month, 1)
        
        date_range = [
            f"2018-01-01,{start_date.strftime('%Y-%m-%d')}"
        ]

        updated_date_range = f"{start_date.strftime('%Y-%m-%d')},{end_date.strftime('%Y-%m-%d')}"

    # Extract Data from API
    df_compiled_game_data = pd.DataFrame()
    df_compiled_platforms = pd.DataFrame()
    df_compiled_stores = pd.DataFrame()
    df_compiled_ratings = pd.DataFrame()
    df_compiled_status = pd.DataFrame()
    df_compiled_tags = pd.DataFrame()
    df_compiled_esrb = pd.DataFrame()
    df_compiled_parent_platform = pd.DataFrame()
    df_compiled_genre = pd.DataFrame()

    for range in date_range:
        continue_extract = True
        page = 1
        while continue_extract:
            if extraction_task == "extract_updates":
                print("Updates Date:")
                print(updated_date_range)
                game_list_resp = get_list_response(RAWG_TOKEN, 
                                        "games", 
                                        page_size=40, 
                                        page=page,
                                        exclude_stores="9",
                                        updated=updated_date_range,
                                        dates=range)
            else:
                game_list_resp = get_list_response(RAWG_TOKEN, 
                                        "games", 
                                        page_size=40, 
                                        page=page,
                                        exclude_stores="9",
                                        dates=range)
            # Unpack Nested API
            output = transform_game_list_api(game_list_resp["results"])
            df_compiled_game_data = pd.concat([df_compiled_game_data,output["game_data"]])
            df_compiled_platforms = pd.concat([df_compiled_platforms,output["platforms"]])
            df_compiled_stores = pd.concat([df_compiled_stores,output["stores"]])
            df_compiled_ratings = pd.concat([df_compiled_ratings,output["detailed_ratings"]])
            df_compiled_status = pd.concat([df_compiled_status,output["status"]])
            df_compiled_tags = pd.concat([df_compiled_tags,output["tags"]])
            df_compiled_esrb = pd.concat([df_compiled_esrb,output["esrb_rating"]])
            df_compiled_parent_platform = pd.concat([df_compiled_parent_platform,output["parent_platform"]])
            df_compiled_genre = pd.concat([df_compiled_genre,output["genres"]])

            if game_list_resp["next"] != None:
                page += 1
            else:
                continue_extract = False

    # Data path which the rate csv data be stored [depends on type of extraction]
    # if extraction_task == "initial_upload":
    #     root_data_directory = os.path.join(os.getcwd(), "data", "initial_upload")
    # elif extraction_task == "extract_new_games":
    #     root_data_directory = os.path.join(os.getcwd(), "data", "monthly_upload", "new_games")
    # elif extraction_task == "extract_updates":
    #     root_data_directory = os.path.join(os.getcwd(), "data", "monthly_upload", "old_games")    
    

    root_data_directory = ti.xcom_pull(task_ids='set_data_directory', key="root_data_directory")
    data_directory = os.path.join(root_data_directory, "raw_data")

    # Store Data and Path Data Path to XCOM
    game_data_path = os.path.join(data_directory, "game_data.csv")
    df_compiled_game_data.to_csv(game_data_path, index=False)

    game_platform_path = os.path.join(data_directory, "game_platform.csv")
    df_compiled_platforms.to_csv(game_platform_path, index=False)
    
    game_store_path = os.path.join(data_directory, "game_store.csv")
    df_compiled_stores.to_csv(game_store_path, index=False)
    
    game_rating_path = os.path.join(data_directory, "game_rating.csv")
    df_compiled_ratings.to_csv(game_rating_path, index=False)
    
    game_status_path = os.path.join(data_directory, "game_status.csv")
    df_compiled_status.to_csv(game_status_path, index=False)

    game_tag_path = os.path.join(data_directory, "game_tag.csv")
    df_compiled_tags.to_csv(game_tag_path, index=False)

    game_esrb_path = os.path.join(data_directory, "game_esrb.csv")
    df_compiled_esrb.to_csv(game_esrb_path, index=False)

    game_parent_platform_path = os.path.join(data_directory, "game_parent_platform.csv")
    df_compiled_parent_platform.to_csv(game_parent_platform_path, index=False)
    
    game_genre_path = os.path.join(data_directory, "game_genre.csv")
    df_compiled_genre.to_csv(game_genre_path, index=False)



def extract_game_detail(**kwargs):
    # Task Instance
    ti = kwargs["ti"]

    # Read Game Data
    root_data_directory = ti.xcom_pull(task_ids='set_data_directory', key="root_data_directory")
    df_game_data = pd.read_csv(os.path.join(root_data_directory, "raw_data", "game_data.csv"))

    # Dictionary mapping each id to slug incase id fails
    dct_id_slug = create_dictionary_from_df(df_game_data, "id", "slug")
    game_list_id = df_game_data["id"].unique().tolist()

    # Extract Data from API
    df_game_details_data = pd.DataFrame()
    df_game_details_metacritic = pd.DataFrame()
    df_game_details_developers = pd.DataFrame()
    df_game_details_publishers = pd.DataFrame()
    df_game_details_reactions = pd.DataFrame()

    index_start = 0
    index_stop = len(game_list_id)

    for i in range(index_start, index_stop):
        game_id = game_list_id[i]
        game_detail_json = get_detail_response(RAWG_TOKEN, "games", game_id)

        if type(game_detail_json) == requests.models.Response:
            game_slug = dct_id_slug[game_id]
            game_detail_json = get_detail_response(RAWG_TOKEN, "games", game_slug)
        
        dct_game_details = transform_game_detail_api(game_detail_json)

        df_game_details_data = pd.concat([df_game_details_data, dct_game_details["game_details_data"]])
        df_game_details_metacritic = pd.concat([df_game_details_metacritic, dct_game_details["metacritic_platforms"]])
        df_game_details_developers = pd.concat([df_game_details_developers, dct_game_details["developers"]])
        df_game_details_publishers = pd.concat([df_game_details_publishers, dct_game_details["publishers"]])
        
        if "reactions" in dct_game_details.keys():
            df_game_details_reactions = pd.concat([df_game_details_reactions, dct_game_details["reactions"]])

    # Data Directory
    data_directory = os.path.join(root_data_directory, "raw_data")

    # Store Data and Path Data Path to XCOM
    game_details_data_path = os.path.join(data_directory, f"game_details_data.csv")
    df_game_details_data.to_csv(game_details_data_path, index=False)
    
    game_details_metacritic_path = os.path.join(data_directory, f"game_details_metacritic.csv")
    df_game_details_metacritic.to_csv(game_details_metacritic_path, index=False)
    
    game_details_developer_path = os.path.join(data_directory, f"game_details_developer.csv")
    df_game_details_developers.to_csv(game_details_developer_path, index=False)
        
    game_details_publisher_path = os.path.join(data_directory, f"game_details_publisher.csv")
    df_game_details_publishers.to_csv(game_details_publisher_path, index=False)
    
    game_details_reaction_path = os.path.join(data_directory, f"game_details_reaction.csv")
    df_game_details_reactions.to_csv(game_details_reaction_path, index=False)



def extract_publisher(**kwargs):
    # Task Instance
    ti = kwargs["ti"]

    # Read Game Details Publisher
    root_data_directory = ti.xcom_pull(task_ids='set_data_directory', key="root_data_directory")
    data_directory = os.path.join(root_data_directory, "raw_data")
    df_game_publisher = pd.read_csv(os.path.join(root_data_directory, "raw_data", "game_details_publisher.csv"))
    lst_of_publishers = df_game_publisher["publisher_id"].unique().tolist()

    # Extract Data from API
    df_publishers = pd.DataFrame()
    for i in range(0, len(lst_of_publishers)):
        id = lst_of_publishers[i]
        publishers_json = get_detail_response(
                                            RAWG_TOKEN, 
                                            "publishers", 
                                            id)
        df_curr_publisher = pd.DataFrame(publishers_json, index=[0])
        df_publishers = pd.concat([df_publishers, df_curr_publisher])

    # Export to raw_data folder
    publisher_data_path = os.path.join(data_directory, "publisher_data.csv")
    df_publishers.to_csv(publisher_data_path, index=False)



def extract_genre(**kwargs):
    # Task Instance
    ti = kwargs["ti"]

    # Data Directory
    root_data_directory = ti.xcom_pull(task_ids='set_data_directory', key="root_data_directory")
    data_directory = os.path.join(root_data_directory, "raw_data")

    # If file already exist in the raw_data folder, terminate
    if "tag_data.csv" in os.listdir(data_directory):
        return

    # Extract Data from API
    genre_resp = get_list_response(RAWG_TOKEN, "genres", page_size=40)
    df_genre = pd.DataFrame(genre_resp["results"])
    df_genre_output = df_genre[["id", "name", "slug"]]
    
    # Export to raw_data folder
    df_genre_output.to_csv(os.path.join(data_directory, "genre_data.csv"), index=False)



def extract_tag(**kwargs):
    # Task Instance
    ti = kwargs["ti"]

    # Data Directory
    root_data_directory = ti.xcom_pull(task_ids='set_data_directory', key="root_data_directory")
    data_directory = os.path.join(root_data_directory, "raw_data")

    # If file already exist in the raw_data folder, terminate
    if "tag_data.csv" in os.listdir(data_directory):
        return

    # Extract Data from API
    df_all_tags = pd.DataFrame()
    continue_extract = True
    page = 1
    while continue_extract:
        tags_resp = get_list_response(RAWG_TOKEN, "tags", page_size=40, page=page)
        df_all_tags = pd.concat([df_all_tags, pd.DataFrame(tags_resp["results"])])
        
        if tags_resp["next"] != None:
            page += 1
        else:
            continue_extract = False
    df_tags_output = df_all_tags[["id", "name", "slug"]].copy()
    
    # Export to raw_data folder
    tag_data_path = os.path.join(data_directory, "tag_data.csv")
    df_tags_output.to_csv(tag_data_path, index=False)



def extract_store(**kwargs):
    # Task Instance
    ti = kwargs["ti"]

    # Data Directory
    root_data_directory = ti.xcom_pull(task_ids='set_data_directory', key="root_data_directory")
    data_directory = os.path.join(root_data_directory, "raw_data")

    # If file already exist in the raw_data folder, terminate
    if "store_data.csv" in os.listdir(data_directory):
        return

    # Extract Data from API
    stores_resp = get_list_response(RAWG_TOKEN, "stores", page_size=40)
    df_store = pd.DataFrame(stores_resp["results"])
    df_store_output = df_store[["id", "name", "domain", "slug"]].copy()
    
    # Export to raw_data folder
    df_store_output.to_csv(os.path.join(data_directory, "store_data.csv"), index=False)



def extract_platform(**kwargs):
    # Task Instance
    ti = kwargs["ti"]

    # Data Directory
    root_data_directory = ti.xcom_pull(task_ids='set_data_directory', key="root_data_directory")
    data_directory = os.path.join(root_data_directory, "raw_data")

    # If file already exist in the raw_data folder, terminate
    if "platform_data.csv" in os.listdir(data_directory):
        return

    # Extract Data from API
    df_all_platforms = pd.DataFrame()
    continue_extract = True
    page = 1
    while continue_extract:
        platforms_resp = get_list_response(RAWG_TOKEN, "platforms", page_size=40, page=page)
        df_all_platforms = pd.concat([df_all_platforms, pd.DataFrame(platforms_resp["results"])])
        
        if platforms_resp["next"] != None:
            page += 1
        else:
            continue_extract = False

    # Export to raw_data folder
    platform_data_path = os.path.join(data_directory, "platform_data.csv")
    df_all_platforms.to_csv(platform_data_path, index=False)



def extract_parent_platform(**kwargs):
    # Task Instance
    ti = kwargs["ti"]

    # Data Directory
    root_data_directory = ti.xcom_pull(task_ids='set_data_directory', key="root_data_directory")
    data_directory = os.path.join(root_data_directory, "raw_data")

    # If file already exist in the raw_data folder, terminate
    if "parent_platform_data.csv" in os.listdir(data_directory):
        return

    # Extract Data from API
    parent_platforms_resp = get_list_response(RAWG_TOKEN, "platforms/lists/parents", page_size=40, ordering="-count")
    df_parent_platforms = pd.DataFrame(parent_platforms_resp["results"])

    df_parent_platform_platform = pd.DataFrame()
    for idx, row in df_parent_platforms.iterrows():
        df_curr = pd.DataFrame(row["platforms"])
        df_curr = df_curr[["id", "name", "slug"]].copy()
        df_curr.rename(columns={"id": "platform_id", "name": "platform_name", "slug": "platform_slug"}, inplace=True)

        df_curr["id"] = row["id"]
        df_parent_platform_platform = pd.concat([df_parent_platform_platform, df_curr])
    
    # Dataframe: Parent Platform Entity Data
    df_parent_platforms_output = df_parent_platforms[["id", "name", "slug"]].drop_duplicates()
    
    # Dataframe: Parent Platform - Parent Relationship Data
    df_parent_platform_platform.rename(columns={"id": "parent_platform_id"}, inplace=True)

    # Export to raw_data folder
    parent_platform_data_path = os.path.join(data_directory, "parent_platform_data.csv")
    df_parent_platforms_output.to_csv(parent_platform_data_path, index=False)
    
    parent_platform_platform_path = os.path.join(data_directory, "parent_platform_platform.csv")
    df_parent_platform_platform.to_csv(parent_platform_platform_path, index=False)