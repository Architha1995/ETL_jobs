# ETL_jobs

## rick_and_morty.py - Introduction

ETL data pipeline that sources data from the Rick and Morty API, transforms it, and loads it into a PostgreSQL database using Apache Airflow for orchestration and DBT for transformation.

### Built with

1. Apache Airflow
2. PostgreSQL
3. DBT
4. Python

### Extract
The python script contains different functions executed by the Python Operator in Apache Airflow. Initially, two tables(public.rick_and_morty_characters,public.rick_and_morty_locations) are created in the postgres database using psycopg2 database adaptor for Python. There are two stages in the DAG that performs GET requests from the API: fetch_characters and fetch_locations. The fetched data is then passed into the next task using XCom.

### Tranform

preprocess_characters and preprocess_locations are the stages where some simple transformations happen.

### Load

load_into_char_table and load_into_loc_table is where the processed data is inserted into the database.

### DBT Transformation

merge_char_locations.sql is a DBT model(dbt_project>model>rickandmorty) which creates a view after merging the contents from both the tables.

### Set up


