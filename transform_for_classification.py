
# ------------------------- Transform Functions for Classification -------------------------

# Import Libraries and Utility Functions
import os 
import numpy as np
import pandas as pd
from datetime import datetime
from pandas.api.types import is_datetime64_any_dtype as is_datetime
from sklearn.preprocessing import MultiLabelBinarizer

# Database Connection Setup
################################################################################################
from dotenv import load_dotenv
dotenv = load_dotenv()
CONNECTION_STRING = os.getenv("MYSQL_CONNECTION_STRING")
from database import Base, ClassificationData
from load import session_engine_from_connection_string, upload_data

# Helper Functions
################################################################################################

def extract_table(table_name):

    # open database session
    session, engine = session_engine_from_connection_string(CONNECTION_STRING)
    conn = engine.connect()

    # SQL command to extract table
    sql_query = f"SELECT * FROM {table_name}"
    table = pd.read_sql(sql_query, session.bind)

    # close database session
    session.close()
    conn.close()

    return table

def bin(rating):
    if rating > 4:   # Exceptional
        return 4
    elif rating > 3: # Recommended
        return 3
    elif rating > 2: # Meh
        return 2
    elif rating > 1: # Skip
        return 1
    else:            # Not Rated
        return 0

def one_hot_encode(df:pd.DataFrame(),rows:str,cols:str) -> pd.DataFrame():
    """
    Helper function to one hot encode categorical entities.
    """
    # replace "-" with "_" to get neater column names
    df[cols] = df[cols].apply(lambda x: x.replace("-","_"))

    # group df by "rows"
    group = pd.DataFrame(df.groupby([rows])[cols].apply(lambda x: tuple(x.values))).reset_index()
    
    # one hot encoding
    mlb = MultiLabelBinarizer()
    content = mlb.fit_transform(group[cols])
    classes = mlb.classes_
    
    new_df = pd.DataFrame(data=content,columns=classes).add_prefix(cols+"_")
    
    # add count as a feature
    new_df[cols+"s_count"] = new_df.sum(axis=1) 
    
    # create id column for joining purposes
    new_df["id"] = group[rows]
    
    return new_df

# Transform Function
################################################################################################

def transform_game_data(ti):

    # read entity table
    entity_game = extract_table("game")

    # user specifications
    columns_to_drop = [
        "slug",
        "name",
        "name_original", 
        #"alternative_names",
        "updated",
        "rating_top",
        "reviews_count", 
        #"ratings_count",
        #"community_rating",
        "metacritic",
        "achievements_count",
        'added_yet',
        'added_owned',
        'added_beaten',
        'added_toplay',
        'added_dropped',
        'added_playing',
        "description_raw",
        #"saturated_color",
        #"dominant_color",
        "background_image",
        "background_image_additional",
        #"clip",
        "reddit_name",
        "reddit_description",
        "reddit_logo",
        "esrb", 
        "score", 
        #"user_game",
        "creators_count",
    ]
    columns_to_rename = {
        'parent_achievements_count':'achievements_count',
        'added':'added_count',
        'movies_count':'movies',
    }
    columns_nan_to_bool = [
        'website',
        'description',
        'reddit_url',
    ]
    columns_num_to_bool = [
        'movies',
    ]
    
    # drop irrelevant columns
    entity_game = entity_game.drop(columns=columns_to_drop)

    # drop rows with rating = 0
    # entity_game = entity_game[entity_game["rating"] > 0]
    
    # rename columns
    entity_game = entity_game.rename(columns=columns_to_rename)

    # convert columns with nan to boolean (nan = false)
    for column in columns_nan_to_bool:
        entity_game[column] = entity_game[column].notna()

    # convert columns with number to boolean (0 = false)
    for column in columns_num_to_bool:
        entity_game[column] = entity_game[column].fillna(0) > 0

    # convert boolean columns to integer
    for column in entity_game.columns:
        if entity_game[column].dtypes.name == 'bool':
            entity_game[column] = entity_game[column].astype(int)

    # seperate release date into day of the week, day of the month, month and year
    if not is_datetime(entity_game['released']):
        entity_game['released'] = entity_game['released'].apply(lambda x: datetime.strptime(x,'%Y-%m-%d'))
    entity_game['day_of_week'] = entity_game['released'].apply(lambda x: x.weekday()+1)
    entity_game['day_of_month'] = entity_game['released'].apply(lambda x: x.day)
    entity_game['month'] = entity_game['released'].apply(lambda x: x.month)
    entity_game['year'] = entity_game['released'].apply(lambda x: x.year)
    entity_game = entity_game.drop(columns=['released'])

    # bin rating column
    entity_game["rating"] = entity_game["rating"].apply(bin)

    # drop NA columns
    entity_game = entity_game.dropna()

    # push to descendent tasks
    ti.xcom_push(value=entity_game.to_dict(), key='game')


def transform_platform_data(ti):

    # read entity and relationship tables
    entity_platform = extract_table("platform").rename(columns={"id":"platform_id","slug":"platform"})
    game_platform = extract_table("game_platform")
    
    # inner join entity and relationship table
    game_platform = game_platform.merge(entity_platform, how="inner", on="platform_id")[["game_id","platform"]]

    # one hot encoding
    ohc_platform = one_hot_encode(game_platform,"game_id","platform")
    
    # push to descendent tasks
    ti.xcom_push(value=ohc_platform.to_dict(), key='game_platform')


def transform_store_data(ti):

    # read entity and relationship tables
    entity_store = extract_table("store").rename(columns={"id":"store_id","slug":"store"})
    game_store = extract_table("game_store")
    
    # inner join entity and relationship table
    game_store = game_store.merge(entity_store, how="inner", on="store_id")[["game_id","store"]]
    
    # one hot encoding
    ohc_store = one_hot_encode(game_store,"game_id","store")

    # push to descendent tasks
    ti.xcom_push(value=ohc_store.to_dict(), key='game_store')


def transform_genre_data(ti):

    # read entity and relationship tables
    entity_genre = extract_table("genre").rename(columns={"id":"genre_id","slug":"genre"})
    game_genre = extract_table("game_genre")

    # inner join entity and relationship table
    game_genre = game_genre.merge(entity_genre, how="inner", on="genre_id")[["game_id","genre"]]

    # one hot encode
    ohc_genre = one_hot_encode(game_genre,"game_id","genre")

    # push to descendent tasks
    ti.xcom_push(value=ohc_genre.to_dict(), key='game_genre')


def merge_data(ti):

    # get tables
    game = pd.DataFrame(ti.xcom_pull(task_ids='transform_game_data', key="game"))
    game_platform = pd.DataFrame(ti.xcom_pull(task_ids='transform_platform_data', key="game_platform"))
    game_store = pd.DataFrame(ti.xcom_pull(task_ids='transform_store_data', key="game_store"))
    game_genre = pd.DataFrame(ti.xcom_pull(task_ids='transform_genre_data', key="game_genre"))

    # merge
    game = game.merge(game_platform, how="inner", on=["id"]) \
               .merge(game_store, how="inner", on=["id"]) \
               .merge(game_genre, how="inner", on=["id"])

    # push to descendent tasks
    ti.xcom_push(value=game.to_dict(), key='data')


def load_data(ti):

    # open database session
    session, engine = session_engine_from_connection_string(CONNECTION_STRING)
    conn = engine.connect()
    
    # Create Table if not created
    Base.metadata.drop_all(engine, [ClassificationData.__table__])
    Base.metadata.create_all(engine, [ClassificationData.__table__])
    
    # local upload
    data = pd.DataFrame(ti.xcom_pull(task_ids='merge_data', key="data"))
    data_directory = ti.xcom_pull(task_ids='set_data_directory', key="data_directory")
    data.to_csv(os.path.join(data_directory, "classification_data.csv"), index=False)

    # sql upload
    upload_data(session, data, ClassificationData)

    # close database session
    session.close()
    conn.close()