# Databricks notebook source
from datetime import date, timedelta

start_date = date.today() - timedelta(1)


silver_adls = 'abfss://silver@farhanstorgedl.dfs.core.windows.net/'
gold_adls = 'abfss://gold@farhanstorgedl.dfs.core.windows.net/'


silver_data = f'{silver_adls}_earthquake_event_silver/'

# COMMAND ----------

pip install reverse_geocoder

# COMMAND ----------

from pyspark.sql.functions import *
from pyspark.sql.types import *

import reverse_geocoder as rg
from datetime import date, timedelta


# COMMAND ----------

df = spark.read.parquet(silver_data).filter(col('Time') > start_date)

# COMMAND ----------

df = df.limit(100)

# COMMAND ----------

def get_country_code(lat, lon):
    """
    Retrive The Country Code For Given Lonitude And Latitude

    parameters:
    lon(float or str): longitude of the location
    lat(float or str): latitude of the location

    Returns:
    str: Country code of the location, retrived using the reverse_geocoding API.

    Example:
    >>> get_country_details(12.9716, 77.5946)
    'FR'
    """

    try:
        cooridnates = (float(lat), float(lon))
        result = rg.search(cooridnates)[0].get('cc')
        print(f'Processed Coordinates: {cooridnates} -> {result}')

        return result
    except Exception as e:
        print(f'Error: {e}')

# COMMAND ----------

get_country_code_udf = udf(get_country_code, StringType())

# COMMAND ----------

get_country_code('12.9716', '77.5946')

# COMMAND ----------

df_with_location = df.withColumn('country_code', get_country_code_udf(col('Latitude'), col('Longitude')))

# COMMAND ----------

df.printSchema()

# COMMAND ----------

df_location_with_sig_class = df_with_location.withColumn('sig_class',
                                                         when(col('Sig') < 100, 'Low')\
                                                        .when((col('sig') >= 100)& (col('sig')<= 500),'Moderate')\
                                                        .otherwise('High'))

# COMMAND ----------

gold_output_path = f'{gold_adls}earthquake_event_gold/'

# COMMAND ----------

df_location_with_sig_class.write.mode('append').parquet(gold_output_path)