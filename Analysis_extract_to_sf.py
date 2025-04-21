# Import libraries (including Snowpark libraries for snowflake connection and Airflow libraries for dag creation)
import json
import requests
import os
import datetime
# Snowflake Library (Note: Snowpark seems to only work with up to Python 3.12 while developing this project I used a virtual environment using Python3.11.9)
from snowflake.snowpark import Session, FileOperation
#Airflow libraries
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago 
from airflow.models.variable import Variable
# Loading environment variables from '.env' file Note: All environment variables will be referenced in allcaps

OWNER = Variable.get('OWNER')
EMAIL = Variable.get('EMAIL')
SFPASS = Variable.get('SFPASS')
SFIDENT = Variable.get('SFIDENT')
SFUSER = Variable.get('SFUSER')
DB = Variable.get('DB')

# Define callables for DAG tasks
def extract_json_to_file():
    today = datetime.date.today().strftime("%d%m%y")
    file = f'/{today}_data.json'
    URL = "https://www.predictit.org/api/marketdata/all/"
    res = requests.get(URL)
    json_data = res.json()
    with open(file, 'w', encoding="utf-8") as json_file:
        json.dump(json_data, json_file, ensure_ascii=False, indent=4)
        json_file.close()

# Potentially useful json reading function
# def read_json_file():
#     file = './raw_data.json'
#     with open(file, 'r', encoding='utf-8') as json_file:
#         json_data = json.load(json_file)
#     return json_data

def load_to_snowflake():
    today = datetime.date.today().strftime("%d%m%y")
    date_file = f'/{today}_data.json'
    connection_parameters = {
        'user' : SFUSER,
        'password' : SFPASS,
        'account' : SFIDENT,
        'role' : 'ACCOUNTADMIN',
        'database' : DB,
        'schema':'public'
    }
    session = Session.builder.configs(connection_parameters).create()
    operation = FileOperation(session)
    operation.put(date_file, f"@jsondata",overwrite=True)
    os.rename(date_file, '/load_json.json')
    new_file = '/load_json.json'
    operation.put(new_file, f"@jsondata",overwrite=True)
    os.remove(new_file)
    session.close()

default_args = {
    'owner' : OWNER,
    'start_date' : days_ago(0),
    'email' : [EMAIL]
}

dag = DAG(
    'ETL-predictit-to-Snowflake',
    default_args=default_args,
    description='DAG that extracts political data and loads it into Snowflake stage as a JSON file',
    schedule_interval=datetime.timedelta(days=1)
)

# Define task for extracting the data from Predictit API
extract_json = PythonOperator(
    task_id='extract_json',
    python_callable=extract_json_to_file,
    dag=dag
)

# Define task for loading JSON data file into the Snowflake stage
load_json_to_snowflake = PythonOperator(
    task_id='load_json_to_snowflake',
    python_callable=load_to_snowflake,
    dag=dag
)

extract_json >> load_json_to_snowflake