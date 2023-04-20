# ------------------------- Load Functions for ETL -------------------------

# Import Libraries and Database Tools
import pandas as pd
import numpy as np
import re
from sqlalchemy import create_engine, update
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


def convert_to_str_for_sql_statements(value):
    if pd.isna(value):
        return "NULL"
    elif type(value) == int or type(value) == float or type(value) == bool:
        return str(value)
    elif type(value) == str:        
        value = re.sub("'", r"\'", value)
        value = re.sub(":", r"\:", value)
        return f"'{value}'"
    else:
        print(f"Edge case: {value} - {type(value)}")
        return value
    

# Convert DF into Table Objects
def convert_df_to_lst_of_table_objects(df, Table):
    '''
    Takes in a dataframe (each column name aligned to a DB table's column name)
    and convert it into a list of Table objects
    '''
    return [Table(**{k: v for k, v in row.items() if not np.array(pd.isnull(v)).any()}) for row in df.to_dict("records")]


# Functions used for Database Insert/Update
################################################################################################

def upload_data(session, df, Table):
    obj = convert_df_to_lst_of_table_objects(df, Table)

    if len(obj) > 0:
        session.add_all(obj)
        session.commit()
        print(f"{len(obj)} objects uploaded into {Table.__tablename__.title()}")
    else:
        print(f"No objects uploaded into {Table.__tablename__.title()}")


def perform_update_insert_or_upload(session, df, Table, composite_keys, value_keys):
    # table name
    table_name = Table.__tablename__
    
    # cast to int for composite keys
    for col in composite_keys:
        df[col] = df[col].astype(int)
    
    records_added = 0
    records_updated = 0
    
    for idx, row in df.iterrows():
        # to sieve out record
        where_statement = " AND ".join(f"{key} = {row[key]}" for key in composite_keys)
        
        # check if exist, if exist perform update, if not insert as new record
        sql_check_exist = f"SELECT 1 FROM {table_name} WHERE {where_statement}"
        df_check_exist_query = pd.read_sql(sql_check_exist, session.bind)
        
        # does not exist, add to table
        if len(df_check_exist_query) == 0:
            df_new_row = row.to_frame().T
            obj_new_row = convert_df_to_lst_of_table_objects(df_new_row, Table)
            session.add_all(obj_new_row)
            session.commit()
            records_added += 1
        
        # exist
        else:
            # if no values to update, ignore
            if len(value_keys) == 0:
                continue
            else:                
                # new values to be updated (will just update all columns for reusability of function)
                set_values_statement = ", ".join(f"`{key}` = {convert_to_str_for_sql_statements(row[key])}" for key in value_keys)
                
                # perform record updates
                sql_update_statement = f"UPDATE {table_name} SET {set_values_statement} WHERE ({where_statement})"
                session.execute(sql_update_statement)
                session.commit()
                records_updated += 1
                
    print(f"{records_added} rows have been added to {table_name}")
    print(f"{records_updated} rows have been updated in {table_name}")
    session.close()
        

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

def create_database_tables():
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
        GameRating.__table__,
    ]
    # Drop All Tables
    Base.metadata.drop_all(engine, table_objects)

    # Create All Tables
    Base.metadata.create_all(engine, table_objects)

    session.close()
    conn.close()


def load_entity_data(**kwargs):
    # Task Instance
    ti = kwargs["ti"]

    # Table to upload
    table = kwargs["table"]
    file_name = kwargs["file_name"]
    
    # Whether it is monthly_update
    monthly_update = kwargs["monthly_update"]

    # Data directory
    root_data_directory = ti.xcom_pull(task_ids='set_data_directory', key="root_data_directory")
    data_directory = os.path.join(root_data_directory, "transformed_data")

    # create session and engine
    session, engine = session_engine_from_connection_string(CONNECTION_STRING)
    conn = engine.connect()

    # Game Entity
    data_path = os.path.join(data_directory, file_name)
    df = pd.read_csv(data_path)
    
    if not monthly_update:
        upload_data(session, df, table)
    else:
        value_col = df.columns.tolist()
        value_col.remove("id")
        perform_update_insert_or_upload(session, df, table, composite_keys=["id"], value_keys=value_col)


def load_game_relationship_data(**kwargs):
    # Task Instance
    ti = kwargs["ti"]

    # Entity (lowercased name of each table)
    entity = kwargs["entity"]
    entity_col_name = f"{entity}_id"

    # Table to upload
    table = kwargs["table"]
    file_name = kwargs["file_name"]
    
    # Whether it is monthly_update
    monthly_update = kwargs["monthly_update"]

    # Data directory
    root_data_directory = ti.xcom_pull(task_ids='set_data_directory', key="root_data_directory")
    data_directory = os.path.join(root_data_directory, "transformed_data")

    # create session and engine
    session, engine = session_engine_from_connection_string(CONNECTION_STRING)
    conn = engine.connect()

    file_path = os.path.join(data_directory, file_name)
    df_relationship = pd.read_csv(file_path)

    lst_of_records = df_relationship[entity_col_name].unique().tolist()

    # SQL Command to check for Records that Exist in Schema
    # perform loading if there are records to be loaded
    if len(lst_of_records) > 0:
        sql_query = f"SELECT id FROM {entity} WHERE id IN {lst_of_records}"
        sql_query = re.sub("\[", "(", sql_query)
        sql_query = re.sub("\]", ")", sql_query)
        df_existing_records = pd.read_sql(sql_query, session.bind)

        # Subset for those records that are in the Entity's DB (Prevent ForeignKey error)
        df_relationship_subset = df_relationship[df_relationship[entity_col_name].isin(df_existing_records.id.tolist())]

        # Upload relationship data
        if not monthly_update:
            upload_data(session, df_relationship_subset, table)
        else:
            value_col = df_relationship_subset.columns.tolist()
            value_col.remove("game_id")
            value_col.remove(entity_col_name)
            perform_update_insert_or_upload(session, df_relationship_subset, table, composite_keys=["game_id", entity_col_name], value_keys=value_col)