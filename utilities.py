import os
import requests
import pandas as pd
import numpy as np
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from database import Base

####################################################################################################
###### EXTRACT
####################################################################################################


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
    
####################################################################################################
###### TRANSFORM
####################################################################################################


def create_dictionary_from_df(df: pd.DataFrame(), key: str, value: str) -> dict():
    """
    Helper Function to convert 2 columns in a DataFrame into a Dictionary
    """
    df = df[[key, value]].drop_duplicates()
    return df.set_index([key])[value].to_dict()


def transform_game_list_api(resp_json):
    """
    Function to transform the json response of RAWG's game (list) API
    Returns a dictionary containing transformed DataFrames:

    Output Keys
    ----------
    game_data:
        Data that are associated to each game

    platforms:
        Platform that supports each game
    
    stores:
        Stores that sell each game

    detailed_ratings:
        Breakdown of ratings given to a game

    status:
        Breakdown of "played" status given to a game by users

    tags:
        Tags associated to a game

    esrb_rating:
        ESRB rating assigned to a game

    parent_platform:
        Parent Platform of game

    genres:
        Genre associated to a game


    Parameters
    ----------
    resp_json: json
        JSON object

    """
    df = pd.DataFrame(resp_json)

    output = {}
    col = ['platforms',
        'stores',
        'ratings',
        'added_by_status',
        'tags',
        'esrb_rating',
        'parent_platforms',
        'genres',
        'short_screenshots'
    ]
    
    # Game Details
    # ======================================================
    df_data = df.drop(col, axis=1)
    output["game_data"] = df_data

    df_platforms = pd.DataFrame()
    df_stores = pd.DataFrame()
    df_ratings = pd.DataFrame()
    df_added_by_status = pd.DataFrame()
    df_tags = pd.DataFrame()
    df_esrb_ratings  = pd.DataFrame()
    df_parent_platforms  = pd.DataFrame()
    df_genres = pd.DataFrame()

    for idx, row in df.iterrows():
        # platform
        # ======================================================
        if row["platforms"] != None:
            for platform in row["platforms"]:
                df_platforms = pd.concat([df_platforms, pd.DataFrame({"game_id": [row["id"]], "platform_id": [platform["platform"]["id"]]})])

        # stores
        # ======================================================
        if row["stores"] != None:
            for store in row["stores"]:
                df_stores = pd.concat([df_stores, pd.DataFrame({"game_id": [row["id"]], "store_id": [store["store"]["id"]]})])

        # ratings
        # ======================================================
        if row["ratings"] != None:
            df_curr_rating = pd.DataFrame(row["ratings"])
            df_curr_rating.dropna(inplace=True)
            df_curr_rating["game_id"] = row["id"]
            df_ratings = pd.concat([df_ratings, df_curr_rating])

        # addedd by status
        # ======================================================
        if row["added_by_status"] != None:
            df_curr_added_by_status = pd.DataFrame(row["added_by_status"], index=[0])
            df_curr_added_by_status.dropna(inplace=True)
            df_curr_added_by_status["game_id"] = row["id"]
            df_added_by_status = pd.concat([df_added_by_status, df_curr_added_by_status])
        
        # tags
        # ======================================================
        if row["tags"] != None:
            for tag in row["tags"]:
                df_tags = pd.concat([df_tags, pd.DataFrame({"game_id": [row["id"]], "tag_id": [tag["id"]]})])

        # esrb ratings
        # ======================================================
        if row["esrb_rating"] != None:
            df_curr_esrb = pd.DataFrame(row["esrb_rating"], index=[0])
            df_curr_esrb.rename(columns={"id": "esrb_id"}, inplace=True)
            df_curr_esrb.dropna(inplace=True)
            df_curr_esrb["game_id"] = row["id"]
            df_esrb_ratings = pd.concat([df_esrb_ratings, df_curr_esrb])
        
        # parent platforms
        # ======================================================
        if row["parent_platforms"] != None and type(row["parent_platforms"]) != float:
            for platform in row["parent_platforms"]:
                df_parent_platforms = pd.concat([df_parent_platforms, pd.DataFrame({"game_id": [row["id"]], "platform": \
                                                                                    [platform["platform"]["id"]]})])

        # genres
        # ======================================================
        if row["genres"] != None:
            df_curr_genre = pd.DataFrame(row["genres"])
            df_curr_genre.rename(columns={"id": "genre_id"}, inplace=True)
            df_curr_genre.dropna(inplace=True)
            df_curr_genre["game_id"] = row["id"]
            df_genres = pd.concat([df_genres, df_curr_genre])

        output["platforms"] = df_platforms
        output["stores"] = df_stores
        output["detailed_ratings"] = df_ratings
        output["status"] = df_added_by_status
        output["tags"] = df_tags
        output["esrb_rating"] = df_esrb_ratings
        output["parent_platform"] = df_parent_platforms
        output["genres"] = df_genres

        return output



def transform_game_detail_api(resp_json):
        """
        Function to transform the json response of RAWG's game (detail) API
        Returns a dictionary containing transformed DataFrames (those data returned in transform_game_list are excluded)

        Output Keys
        ----------
        game_details_data:
            Detailed Data that are associated to each game

        metacritic_platforms:
            Metacritic Scores across platforms that supports each game
        
        reactions:
            User reactions to a game

        developers:
            Developers of a game

        publishers:
            Publishers of a game

        Parameters
        ----------
        resp_json: json
            JSON object

        """
        output = {}
        lst_or_dct_fields = [ 
            "metacritic_platforms",
            "ratings",
            "reactions",
            "added_by_status",
            "parent_platforms",
            "platforms",
            "stores",
            "developers",
            "genres",
            "tags",
            "publishers",
            "esrb_rating",
            "alternative_names" # ignore
        ]

        # Game Details Data
        # ======================================================
        df = pd.DataFrame({k:v for k,v in resp_json.items() if k not in lst_or_dct_fields}, index=[0])
        output["game_details_data"] = df

        # clean metacritic platforms
        # ======================================================
        if resp_json["metacritic_platforms"] != None:
            df_metacritic = pd.DataFrame(resp_json["metacritic_platforms"])
            if len(df_metacritic) > 0:
                df_metacritic["platform_id"] = df_metacritic["platform"].apply(lambda x: x["platform"])
                df_metacritic["game_id"] = resp_json["id"]
            output["metacritic_platforms"] = df_metacritic

        # reactions
        # ======================================================
        if resp_json[ "reactions"] != None:
            df_reactions = pd.DataFrame(resp_json["reactions"], index=["count"]).T.reset_index(drop=False).rename(columns={"index": "reaction_id"})
            df_reactions["game_id"] = resp_json["id"]
            output["reactions"] = df_reactions


        # developers
        # ======================================================
        if resp_json[ "developers"] != None:
            df_developers = pd.DataFrame({"developer_id": [record["id"] \
                                             for record in resp_json["developers"]]})
            df_developers["game_id"] = resp_json["id"]
            output["developers"] = df_developers
        
        # publishers
        # ======================================================
        if resp_json[ "publishers"] != None:
            df_publishers = pd.DataFrame({"publisher_id": [record["id"] \
                                             for record in resp_json["publishers"]]})
            df_publishers["game_id"] = resp_json["id"]
            output["publishers"] = df_publishers

        return output



####################################################################################################
###### Load
####################################################################################################


# Used during DB Querying
def convert_row_to_dict(row, Table):
    '''
    Convert an SQLAlchemy Row into a Dictionary
    '''
    lst_of_col = [col.name for col in Table.__table__.columns]

    if type(row) == Table:  # Row obtained from .get()
        dct = { col : getattr(row, col) for col in lst_of_col}
        return dct

    # Obtains from Rows obtained from .all()
    obj = list(dict(row._mapping).values())[0]
    dct = { col : getattr(obj, col) for col in lst_of_col}

    return 


# Session, Engine
def session_engine_from_connection_string(conn_string):
    '''
    Takes in a DB Connection String
    Return a tuple: (session, engine)
    e.g. session, engine = session_engine_from_connection_string(string)
    '''
    engine = create_engine(conn_string)
    Base.metadata.bind = engine
    DBSession = sessionmaker(bind=engine)
    return DBSession(), engine


# Convert DF into Table Objects
def convert_df_to_lst_of_table_objects(df, Table):
    '''
    Takes in a dataframe (each column name aligned to a DB table's column name)
    and convert it into a list of Table objects
    '''
    return [Table(**{k: v for k, v in row.items() if not np.array(pd.isnull(v)).any()}) for row in df.to_dict("records")]