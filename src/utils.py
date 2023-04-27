import json
import sys
from neo4j import GraphDatabase
from src.exception import CustomException
import pandas as pd


def save_json(json_obj, file_path):
    with open("daraz_scraped_data_v3.json", "w") as f:
        json.dump(json_obj, f)

def load_json(file_path):
    with open(file_path, "r") as f:
        return json.load(f)

def load_csv(file_path):
    with open(file_path, "r") as f:
        return pd.read_csv(file_path)

def establish_db_connection(url, auth):
    return GraphDatabase.driver(url,auth=auth)


def is_connected(driver):
    # Verify the connectivity
    try:
        driver.verify_connectivity()
    except Exception as e:
        raise CustomException("Unable to establish connection to the neo4j db", sys)