# Import libraries (including Snowpark libraries for snowflake connection and Airflow libraries for dag creation)
import json
import requests
from dotenv import load_dotenv
import os
import datetime
# Snowflake Library (Note: Snowpark seems to only work with up to Python 3.12 while developing this project I used a virtual environment using Python3.11.9)
import snowflake.connector 
# Airflow libraries
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago 

# Loading environment variables from '.env' file Note: All environment variables will be referenced in allcaps
load_dotenv()
OWNER = os.environ['OWNER']
EMAIL = os.environ['EMAIL']
SFPASS = os.environ['SFPASS']
SFIDENT = os.environ['SFIDENT']
SFUSER = os.environ['SFUSER']
DB = os.environ['DB']

# Define callables for DAG tasks
def extract_json_to_file():
    file = f'./raw_data.json'
    URL = "https://www.predictit.org/api/marketdata/all/"
    res = requests.get(URL)
    json_data = res.json()
    with open(file, 'w', encoding="utf-8") as json_file:
        json.dump(json_data, json_file, ensure_ascii=False, indent=4)
        json_file.close()


def read_json_file():
    file = './raw_data.json'
    with open(file, 'r', encoding='utf-8') as json_file:
        json_data = json.load(json_file)
    return json_data

def transform_json():
    raw_json = read_json_file()
    pass

def load_to_snowflake():
    conn = snowflake.connector.connect(
        user=SFUSER,
        password=SFPASS,
        account=SFIDENT,
        role='ACCOUNTADMIN',
        database=DB,
        schema='public'
    )
    cur = conn.cursor()
    conn.close()
    # today = datetime.date.today().strftime('%d%m%y')
    # pass

load_to_snowflake()

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

# Define task for transforming JSON data into a standardized form
transform_json = PythonOperator(
    task_id='transform_json',
    python_callable=transform_json,
    dag=dag
)

# Define task for loading JSON data file into the Snowflake stage
load_json_to_snowflake = PythonOperator(
    task_id='load_json_to_snowflake',
    python_callable=load_to_snowflake,
    dag=dag
)