# Databricks notebook source
from datetime import date, timedelta

start_date = date.today() - timedelta(1)


bronze_adls = 'abfss://bronze@farhanstorgedl.dfs.core.windows.net/'
silver_adls = 'abfss://silver@farhanstorgedl.dfs.core.windows.net/'

# COMMAND ----------

from pyspark.sql.functions import *
from pyspark.sql.types import *


from datetime import date, timedelta


# COMMAND ----------

df = spark.read.option('multiline', 'true').json(f'{bronze_adls}{start_date}_earthquake_data.json')


# COMMAND ----------

df.head()

# COMMAND ----------

 df.display()

# COMMAND ----------

df = (
    df.select(
        'id',
        col('geometry.coordinates').getItem(0).alias('Longitude'),
        col('geometry.coordinates').getItem(1).alias('Latitude'),
        col('geometry.coordinates').getItem(2).alias('Elevation'),
        col('properties.title').alias('Title'),
        col('properties.place').alias('Place'),
        col('properties.sig').alias('Sig'),
        col('properties.mag').alias('Mag'),
        col('properties.magType').alias('MagType'),
        col('properties.time').alias('Time'),
        col('properties.updated').alias('Updated'),
    ))

# COMMAND ----------

df = df.withColumn('Longitude', when(isnull(col('Longitude')), 0).otherwise(col('Longitude')))\
        .withColumn('Latitude', when(isnull(col('Latitude')), 0).otherwise(col('Latitude')))\
        .withColumn('Time', when(isnull(col('Time')), 0).otherwise(col('Time')))

# COMMAND ----------

df = df.withColumn('Time',(col('Time')/1000).cast(TimestampType()))\
        .withColumn('Updated',(col('Updated')/1000).cast(TimestampType()))

# COMMAND ----------

silver_output_path = f'{silver_adls}_earthquake_event_silver/'

# COMMAND ----------

df.write.mode('append').parquet(silver_output_path)