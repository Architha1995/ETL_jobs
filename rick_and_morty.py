from airflow import DAG
from airflow.operators.python import PythonOperator

# from schlalchemy import create_engine
import psycopg2
from psycopg2.extras import execute_batch

from datetime import datetime
import requests


def fetch_data_from_api(endpoint):
    url = f"https://rickandmortyapi.com/api/{endpoint}"
    response = requests.get(url)
    data = response.json()["results"]
    return data


def preprocess_data_char(char_dict_list):
    processed_char_dict_list = []
    for i in range(len(char_dict_list)):
        for key, value in char_dict_list[i].items():
            if value is None:
                char_dict_list[key] = ""
        processed_dict = {}
        processed_dict = {
            "id": char_dict_list[i]["id"],
            "name": char_dict_list[i]["name"],
            "status": char_dict_list[i]["status"],
            "species": char_dict_list[i]["species"],
            "type": char_dict_list[i]["type"],
            "gender": char_dict_list[i]["gender"],
            "originName": char_dict_list[i]["origin"]["name"],
            "originUrl": char_dict_list[i]["origin"]["url"],
            "locationName": char_dict_list[i]["location"]["name"],
            "locationUrl": char_dict_list[i]["location"]["url"],
            "image": char_dict_list[i]["image"],
            "episodes": char_dict_list[i]["episode"],
            "url": char_dict_list[i]["url"],
            "created": char_dict_list[i]["created"],
        }
        processed_char_dict_list.append(processed_dict)
    return processed_char_dict_list


def preprocess_data_location(loc_dict_list):
    processed_loc_dict_list = []
    for i in range(len(loc_dict_list)):
        for key, value in loc_dict_list[i].items():
            if value is None:
                loc_dict_list[key] = ""
        processed_dict = {}
        processed_dict = {
            "id": loc_dict_list[i]["id"],
            "name": loc_dict_list[i]["name"],
            "type": loc_dict_list[i]["type"],
            "dimension": loc_dict_list[i]["dimension"],
            "residents": loc_dict_list[i]["residents"],
            "url": loc_dict_list[i]["url"],
            "created": loc_dict_list[i]["created"],
        }
        processed_loc_dict_list.append(processed_dict)
    return processed_loc_dict_list


def get_connection():
    try:
        conn = psycopg2.connect(
            dbname="postgres",
            user="postgres",
            password="Parvathy@1",
            host="localhost",
            port="5433",
        )
        return conn
    except:
        print("Unable to connect to database")


def create_table(connection, create_table_sql):
    cursor = connection.cursor()
    try:
        cursor.execute(create_table_sql)
        connection.commit()
    except psycopg2.Error as e:
        print(f"Error creating table: {e}")
        connection.rollback()
    finally:
        cursor.close()


def insert_dicts_into_table(connection, table_name, dict_list):
    if not dict_list:
        return
    keys = dict_list[0].keys()
    columns = ", ".join(keys)
    values_substring = ", ".join(["%s"] * len(keys))
    insert_query = f"INSERT INTO {table_name} ({columns}) VALUES ({values_substring})"
    values = [tuple(d.values()) for d in dict_list]
    cursor = connection.cursor()
    try:
        execute_batch(cursor, insert_query, values)
        connection.commit()
    except psycopg2.Error as e:
        print(f"Error: {e}")
        connection.rollback()
    finally:
        cursor.close()


# characters = fetch_data_from_api("character")
# prep_char_list = preprocess_data_char(characters)
locations = fetch_data_from_api("location")
print(locations)
prep_loc_list = preprocess_data_location(locations)
connection = get_connection()
create_char_table_sql = """
        CREATE TABLE IF NOT EXISTS public.rick_and_morty_characters (
            id SERIAL PRIMARY KEY,
            name VARCHAR(255),
            status VARCHAR(50),
            species VARCHAR(50),
            type VARCHAR(50),
            gender VARCHAR(50),
            originName VARCHAR(255),
            originUrl VARCHAR(255),
            locationName VARCHAR(255),
            locationUrl VARCHAR(255),
            image VARCHAR(255),
            episodes TEXT[],
            url VARCHAR(255),
            created TIMESTAMP
        );
        """
create_loc_table_sql = """
        CREATE TABLE IF NOT EXISTS public.rick_and_morty_locations (
            id SERIAL PRIMARY KEY,
            name VARCHAR(255),
            type VARCHAR(50),
            dimension VARCHAR(255),
            residents TEXT[],
            url VARCHAR(255),
            created TIMESTAMP
        );
        """
create_table(connection, create_char_table_sql)
create_table(connection, create_loc_table_sql)
# char_tabl_name = "public.rick_and_morty_characters"
loc_table_name = "public.rick_and_morty_locations"
# insert_dicts_into_table(connection, char_tabl_name, prep_char_list)
insert_dicts_into_table(connection, loc_table_name, prep_loc_list)
# locations = fetch_data_from_api("location")
# print(locations)
# with DAG(
#     "rick_and_morty_etl",
#     start_date=datetime(2021, 1, 1),
#     schedule_interval="@once",
#     catchup=False,
# ) as dag:
#     fetch_characters = PythonOperator(
#         task_id="fetch_characters",
#         python_callable=fetch_data_from_api,
#         op_args=["character"],
#     )
#     fetch_locations = PythonOperator(
#         task_id="fetch_locations",
#         python_callable=fetch_data_from_api,
#         op_args=["location"],
#     )

# fetch_characters >> fetch_locations


"""
id, name, type, dimension , residents(list), url,created , 
char - id,name,status,species,type,gender,origin ( name, url), location(name,url),image(string),episode(list),url(string),created(timestamp)
"""
