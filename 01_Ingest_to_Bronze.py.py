# Databricks notebook source
tiers = [ "bronze", "silver", "gold"]
adls_path = {tier: f"abfss://{tier}@farhanstorgedl.dfs.core.windows.net/" for tier in tiers}


# Acessing Paths
bronze_adls = adls_path["bronze"]
silver_adls = adls_path["silver"]
gold_adls = adls_path["gold"]


dbutils.fs.ls(bronze_adls)
dbutils.fs.ls(silver_adls)
dbutils.fs.ls(gold_adls)

# COMMAND ----------

import requests
import json
from datetime import date , timedelta

# COMMAND ----------

start_date = date.today() - timedelta(1)
end_date = date.today()

# COMMAND ----------

start_date , end_date

# COMMAND ----------

url = f'https://earthquake.usgs.gov/fdsnws/event/1/query?format=geojson&starttime={start_date}&endtime={end_date}'

try:
    response = requests.get(url)

    response.raise_for_status()


    data = response.json().get('features', [])

    if not data:
        print("No earthquake data found for the specified date range.")
    else:
        file_path = f'{bronze_adls}/{start_date}_earthquake_data.json'

        json_data = json.dumps(data, indent=4)
        dbutils.fs.put(file_path, json_data, overwrite=True)
        print(f'Earthquake data saved to {file_path}')

except requests.exceptions.RequestException as e:
        print(f'Error fetching earthquake data: {e}')

# COMMAND ----------

data


# COMMAND ----------

