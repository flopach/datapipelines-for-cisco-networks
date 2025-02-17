from typing import List
import requests
import json
from requests.auth import HTTPBasicAuth
import sqlite3

from dagster import asset
from dagster import AssetExecutionContext
from dagster import AssetIn
from dagster import AssetSelection
from dagster import define_asset_job
from dagster import Definitions
from dagster import ScheduleDefinition

# Cisco DNA Center credentials
DNAC_URL = "https://sandboxdnac.cisco.com"
DNAC_USER = "devnetuser"
DNAC_PASS = "Cisco123!"

# Function to obtain a token
class Authentication:

    @staticmethod
    def get_jsessionid(vmanage_host, vmanage_port, username, password):
        api = "/j_security_check"
        base_url = "https://%s:%s"%(vmanage_host, vmanage_port)
        url = base_url + api
        payload = {'j_username' : username, 'j_password' : password}

        response = requests.post(url=url, data=payload, verify=False)
        try:
            cookies = response.headers["Set-Cookie"]
            jsessionid = cookies.split(";")
            return(jsessionid[0])
        except:
            print("No valid JSESSION ID returned\n")
            exit()

    @staticmethod
    def get_token(vmanage_host, vmanage_port, jsessionid):
        headers = {'Cookie': jsessionid}
        base_url = "https://%s:%s"%(vmanage_host, vmanage_port)
        api = "/dataservice/client/token"
        url = base_url + api      
        response = requests.get(url=url, headers=headers, verify=False)
        if response.status_code == 200:
            return(response.text)
        else:
            return None

def get_sdwan_token():
    Auth = Authentication()
    jsessionid = Auth.get_jsessionid("sandbox-sdwan-2.cisco.com",443,"devnetuser","RG!_Yw919_83")
    token = Auth.get_token("sandbox-sdwan-2.cisco.com",443,jsessionid)

    base_url = "https://sandbox-sdwan-2.cisco.com"

    if token is not None:
        header = {'Content-Type': "application/json",'Cookie': jsessionid, 'X-XSRF-TOKEN': token}
    else:
        header = {'Content-Type': "application/json",'Cookie': jsessionid}

# Function to get network devices
def get_network_devices(token):
    url = f"{DNAC_URL}/dna/intent/api/v1/device-health"
    headers = {
        "Content-Type": "application/json",
        "Accept": "application/json",
        "X-Auth-Token": token
    }
    
    response = requests.get(url, headers=headers,verify=False)
    
    if response.status_code == 200:
        devices = response.json()["response"]
        return devices
    else:
        raise Exception(f"Failed to get network devices: {response.status_code} - {response.text}")


@asset(key="get_data_from_catalyst_center", group_name="catalyst_center")
def get_data_from_catalyst_center(context: AssetExecutionContext):
    """
    Obtain token and get data from Catalyst Center
    """
    
    token = get_sdwan_token()
    devices = get_network_devices(token)

    context.log.info(devices)
    return devices


@asset(
    ins={"upstream": AssetIn(key="get_data_from_catalyst_center")},
    group_name="catalyst_center",
)
def clean_data(context: AssetExecutionContext, upstream: List):
    """
    Cleaning the data
    """
    cleaned_data = []

    for device in upstream:

        # data cleansing
        if int(device["issueCount"]) > 0 or int(device["overallHealth"]) < 10:
            device = {
                "name" : device["name"],
                "ipAddress" : device["ipAddress"],
                "issueCount" : device["issueCount"],
                "overallHealth" : device["overallHealth"]
            }
            cleaned_data.append(device)

    context.log.info(f"Output data is: {cleaned_data}")
    return cleaned_data


@asset(
    ins={"second_upstream": AssetIn("clean_data")},
    group_name="catalyst_center",
)
def insert_into_database(context: AssetExecutionContext, second_upstream: List):
    """
    Insert the data into the database
    """
    # Connect to SQLite database (or create it if it doesn't exist)
    conn = sqlite3.connect('db/example.db')
    cursor = conn.cursor()

    # Create a new SQLite table with columns id, name, and age
    cursor.execute('''CREATE TABLE IF NOT EXISTS devices (
    name TEXT PRIMARY KEY,
    ipAddress INTEGER,
    issueCount INTEGER,
    overallHealth INTEGER)
    ''')

    # Insert JSON data into the SQLite database
    for entry in second_upstream:
        cursor.execute('''
        INSERT INTO devices (name, ipAddress, issueCount,overallHealth) VALUES (?, ?, ?, ?)
        ''', (entry['name'], entry['ipAddress'], entry['issueCount'],entry['overallHealth']))

    # Commit the changes and close the connection
    conn.commit()
    conn.close()

    print("Data inserted successfully")
    return "data"


defs = Definitions(
    assets=[get_data_from_catalyst_center, clean_data, insert_into_database],
    jobs=[
        define_asset_job(
            name="hello_dagster_job",
            selection=AssetSelection.groups("catalyst_center"),
        )
    ],
    schedules=[
        ScheduleDefinition(
            name="hello_dagster_schedule",
            job_name="hello_dagster_job",
            cron_schedule="* * * * *",
        )
    ],
)
