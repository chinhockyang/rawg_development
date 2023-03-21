# ------------------------- Load Functions for ETL -------------------------

# Import Libraries and Database Tools
import pandas as pd
import numpy as np
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from database import *

# Database Connection Setup
################################################################################################
import os
from dotenv import load_dotenv
dotenv = load_dotenv()
CONNECTION_STRING = os.getenv("MYSQL_CONNECTION_STRING")


# Utility Functions
################################################################################################

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

    return dct


# Convert DF into Table Objects
def convert_df_to_lst_of_table_objects(df, Table):
    '''
    Takes in a dataframe (each column name aligned to a DB table's column name)
    and convert it into a list of Table objects
    '''
    return [Table(**{k: v for k, v in row.items() if not np.array(pd.isnull(v)).any()}) for row in df.to_dict("records")]


def upload_data(session, data_path, Table):
    df = pd.read_csv(data_path)
    obj = convert_df_to_lst_of_table_objects(df, Table)

    if len(obj) > 0:
        session.add_all(obj)
        session.commit()
        print(f"{len(obj)} objects uploaded into {Table.__tablename__.title()}")
    else:
        print(f"Fail to upload into {Table.__tablename__.title()}")


# Database Communication Sessions Instantiation
################################################################################################

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


# Python Functions of Python Operators
################################################################################################

def load_data(**kwargs):
    # Task Instance
    ti = kwargs["ti"]
    
    # Data directory
    root_data_directory = ti.xcom_pull(task_ids='set_data_directory', key="root_data_directory")
    data_directory = os.path.join(root_data_directory, "transformed_data")

    # create session and engine
    session, engine = session_engine_from_connection_string(CONNECTION_STRING)
    conn = engine.connect()

    # Initial Upload
    if "initial_upload" in root_data_directory:
        print("Recreating Schema")

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
            GameRating.__table__,
        ]
        # Drop All Tables
        Base.metadata.drop_all(engine, table_objects)

        # Create All Tables
        Base.metadata.create_all(engine, table_objects)

    # Game Entity
    game_data_path = os.path.join(data_directory, "entity_game.csv")
    upload_data(session, game_data_path, Game)

    # Publisher Entity
    publisher_data_path = os.path.join(data_directory, "entity_publisher.csv")
    upload_data(session, publisher_data_path, Publisher)

    # Genre Entity
    genre_data_path = os.path.join(data_directory, "entity_genre.csv")
    upload_data(session, genre_data_path, Genre)

    # Tag Entity
    tag_data_path = os.path.join(data_directory, "entity_tag.csv")
    upload_data(session, tag_data_path, Tag)

    # Store Entity
    store_data_path = os.path.join(data_directory, "entity_store.csv")
    upload_data(session, store_data_path, Store)

    # Parent Platform Entity
    parent_platform_data_path = os.path.join(data_directory, "entity_parent_platform.csv")
    upload_data(session, parent_platform_data_path, ParentPlatform)

    # Platform Entity
    platform_data_path = os.path.join(data_directory, "entity_platform.csv")
    upload_data(session, platform_data_path, Platform)

    # Rating
    rating_data_path = os.path.join(data_directory, "entity_rating.csv")
    upload_data(session, rating_data_path, Rating)

    # GamePublisher
    game_publisher_data_path = os.path.join(data_directory, "rs_game_publisher.csv")
    upload_data(session, game_publisher_data_path, GamePublisher)

    # GameGenre
    game_genre_data_path = os.path.join(data_directory, "rs_game_genre.csv")
    upload_data(session, game_genre_data_path, GameGenre)

    # GameTag 
    # ----------------------------------------------------------------------[SPECIAL CASE]: Tag info is very unstable (Some tags in Game-Tag isn't even in Tag API)
    game_tag_data_path = os.path.join(data_directory, "rs_game_tag.csv")
    df_tag = pd.read_csv(tag_data_path)
    df_game = pd.read_csv(game_data_path)
    df_game_tag = pd.read_csv(game_tag_data_path)
    df_game_tag = df_game_tag[df_game_tag["tag_id"].isin(df_tag["id"].unique().tolist())]
    df_game_tag = df_game_tag[df_game_tag["game_id"].isin(df_game["id"].unique().tolist())]
    obj = convert_df_to_lst_of_table_objects(df_game_tag, GameTag)

    if len(obj) > 0:
        session.add_all(obj)
        session.commit()
        print(f"{len(obj)} objects uploaded into {GameTag.__tablename__.title()}")
    else:
        print(f"Fail to upload into {GameTag.__tablename__.title()}")

    # GameStore
    game_store_data_path = os.path.join(data_directory, "rs_game_store.csv")
    upload_data(session, game_store_data_path, GameStore)

    # GamePlatform
    game_platform_data_path = os.path.join(data_directory, "rs_game_platform.csv")
    upload_data(session, game_platform_data_path, GamePlatform)    

    # GameRating
    game_rating_data_path = os.path.join(data_directory, "rs_game_rating.csv")
    upload_data(session, game_rating_data_path, GameRating)