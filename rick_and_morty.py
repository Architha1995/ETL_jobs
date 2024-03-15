from airflow import DAG
from airflow.operators.python import PythonOperator
import psycopg2
from psycopg2.extras import execute_batch
from datetime import datetime
import requests


def get_connection():
    """Returns a connection to the postgres database"""
    try:
        conn = psycopg2.connect(
            dbname="postgres",
            user="postgres",
            password="adminpostgres",
            host="localhost",
            port="5433",
        )
        return conn
    except:
        print("Unable to connect to database")


def create_table(connection, create_table_sql):
    """Create a new table in the postgres database"""
    cursor = connection.cursor()
    try:
        cursor.execute(create_table_sql)
        connection.commit()
    except psycopg2.Error as e:
        print(f"Error creating table: {e}")
        connection.rollback()
    finally:
        cursor.close()


def fetch_data_from_api(endpoint, **kwargs):
    """Returns a list of dictionary items from the rick and morty API response"""
    ti = kwargs["ti"]
    item_list = []
    page_num = 1
    try:
        while True:
            url = f"https://rickandmortyapi.com/api/{endpoint}?page={page_num}"
            # Added this to avoid proxy issues of accessing the API
            session = requests.Session()
            session.trust_env = False
            response = session.get(url, timeout=30)
            if response.status_code == 200:
                data = response.json()["results"]
                item_list.append(data)
                # checking if the last page has reached
                if response.json()["info"]["next"] == None:
                    break
            else:
                print(
                    "Unable to get data items. Response status code: ",
                    response.status_code,
                )
                print(response.reason)
            page_num += 1
        ti.xcom_push("item_list", item_list)
    except:
        print("error occurred while fetching data")
        raise Exception("Failed to fetch data")


def preprocess_data_char(**kwargs):
    """Returns a processed list of dictionary items after mapping the columns for character table"""
    ti = kwargs["ti"]
    char_dict_full_items = ti.xcom_pull(task_ids="fetch_characters", key="item_list")
    processed_char_dict_list = []
    for j in range(len(char_dict_full_items)):
        char_dict_list = char_dict_full_items[j]
        for i in range(len(char_dict_list)):
            for key, value in char_dict_list[i].items():
                if value is None:
                    char_dict_list[key] = ""
            processed_dict = {}
            processed_dict = {
                "character_id": char_dict_list[i]["id"],
                "character_name": char_dict_list[i]["name"],
                "character_status": char_dict_list[i]["status"],
                "character_species": char_dict_list[i]["species"],
                "character_type": char_dict_list[i]["type"],
                "character_gender": char_dict_list[i]["gender"],
                "character_origin_name": char_dict_list[i]["origin"]["name"],
                "character_origin_url": char_dict_list[i]["origin"]["url"],
                "character_location_name": char_dict_list[i]["location"]["name"],
                "character_location_url": char_dict_list[i]["location"]["url"],
                "character_image": char_dict_list[i]["image"],
                "character_episodes": char_dict_list[i]["episode"],
                "character_url": char_dict_list[i]["url"],
                "character_created": char_dict_list[i]["created"],
            }
            processed_char_dict_list.append(processed_dict)
    ti.xcom_push("processed_char_dict_list", processed_char_dict_list)


def preprocess_data_location(**kwargs):
    """Returns a processed list of dictionary items after mapping the columns for location table"""
    ti = kwargs["ti"]
    loc_dict_full_items = ti.xcom_pull(task_ids="fetch_locations", key="item_list")
    processed_loc_dict_list = []
    for j in range(len(loc_dict_full_items)):
        loc_dict_list = loc_dict_full_items[j]
        for i in range(len(loc_dict_list)):
            for key, value in loc_dict_list[i].items():
                if value is None:
                    loc_dict_list[key] = ""
            processed_dict = {}
            processed_dict = {
                "location_id": loc_dict_list[i]["id"],
                "location_name": loc_dict_list[i]["name"],
                "location_type": loc_dict_list[i]["type"],
                "location_dimension": loc_dict_list[i]["dimension"],
                "location_residents": loc_dict_list[i]["residents"],
                "location_url": loc_dict_list[i]["url"],
                "location_created": loc_dict_list[i]["created"],
            }
            processed_loc_dict_list.append(processed_dict)
    ti.xcom_push("processed_loc_dict_list", processed_loc_dict_list)


def insert_dicts_into_table(connection, table_name, **kwargs):
    """Performs insert query into rick and morty tables in postgres database"""
    ti = kwargs["ti"]
    if table_name == "public.rick_and_morty_characters":
        dict_list = ti.xcom_pull(
            task_ids="preprocess_characters", key="processed_char_dict_list"
        )
    elif table_name == "public.rick_and_morty_locations":
        dict_list = ti.xcom_pull(
            task_ids="preprocess_locations", key="processed_loc_dict_list"
        )

    # getting the list of keys from individual dict
    keys = dict_list[0].keys()
    columns = ", ".join(keys)
    # placeholder for the values for insert query
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


# create queries for creating postgresql tables
create_char_table_sql = """
        CREATE TABLE IF NOT EXISTS public.rick_and_morty_characters (
            character_id SERIAL PRIMARY KEY,
            character_name VARCHAR(255),
            character_status VARCHAR(50),
            character_species VARCHAR(50),
            character_type VARCHAR(50),
            character_gender VARCHAR(50),
            character_origin_name VARCHAR(255),
            character_origin_url VARCHAR(255),
            character_location_name VARCHAR(255),
            character_location_url VARCHAR(255),
            character_image VARCHAR(255),
            character_episodes TEXT[],
            character_url VARCHAR(255),
            character_created TIMESTAMP
        );
        """
create_loc_table_sql = """
        CREATE TABLE IF NOT EXISTS public.rick_and_morty_locations (
            location_id SERIAL PRIMARY KEY,
            location_name VARCHAR(255),
            location_type VARCHAR(50),
            location_dimension VARCHAR(255),
            location_residents TEXT[],
            location_url VARCHAR(255),
            location_created TIMESTAMP
        );
        """
# creating connection with postgresql database
connection = get_connection()
create_table(connection, create_char_table_sql)
create_table(connection, create_loc_table_sql)
char_tabl_name = "public.rick_and_morty_characters"
loc_table_name = "public.rick_and_morty_locations"

with DAG(
    "rick_and_morty_extract_dag",
    start_date=datetime(2021, 1, 1),
    schedule_interval="@once",
    catchup=False,
) as dag:

    fetch_characters = PythonOperator(
        task_id="fetch_characters",
        python_callable=fetch_data_from_api,
        op_args=["character"],
    )
    preprocess_characters = PythonOperator(
        task_id="preprocess_characters",
        python_callable=preprocess_data_char,
    )
    load_into_char_table = PythonOperator(
        task_id="load_into_char_table",
        python_callable=insert_dicts_into_table,
        op_args=[connection, char_tabl_name],
    )
    fetch_locations = PythonOperator(
        task_id="fetch_locations",
        python_callable=fetch_data_from_api,
        op_args=["location"],
    )
    preprocess_locations = PythonOperator(
        task_id="preprocess_locations",
        python_callable=preprocess_data_location,
    )
    load_into_loc_table = PythonOperator(
        task_id="load_into_loc_table",
        python_callable=insert_dicts_into_table,
        op_args=[connection, loc_table_name],
    )


(
    fetch_characters
    >> preprocess_characters
    >> load_into_char_table
    >> fetch_locations
    >> preprocess_locations
    >> load_into_loc_table
)
