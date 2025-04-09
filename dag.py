# from airflow.models import DAG 
import json
import requests



def extract_json_to_file():
    file = './raw_data.json'
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
    return json_data['markets']

