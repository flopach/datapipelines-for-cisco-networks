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
def get_dnac_token():
    url = f"{DNAC_URL}/dna/system/api/v1/auth/token"
    headers = {
        "Content-Type": "application/json",
        "Accept": "application/json"
    }
    
    response = requests.post(url, headers=headers, auth=HTTPBasicAuth(DNAC_USER, DNAC_PASS),verify=False)
    
    if response.status_code == 200:
        token = response.json()["Token"]
        return token
    else:
        raise Exception(f"Failed to obtain token: {response.status_code} - {response.text}")

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
    
    token = get_dnac_token()
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
    conn = sqlite3.connect('example.db')
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
            name="catc_devicehealth_job",
            selection=AssetSelection.groups("catalyst_center"),
        )
    ],
    schedules=[
        ScheduleDefinition(
            name="catc_catchall_schedule",
            job_name="catc_devicehealth_job",
            cron_schedule="* * * * *",
        )
    ],
)
