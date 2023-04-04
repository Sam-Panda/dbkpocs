# Databricks notebook source
import dlt
from pyspark.sql.functions import *
folder_path = "/mnt/dlt-container/streamcsvfiles"

@dlt.table
def raw_table():
  df = spark.readStream.format("cloudFiles") \
      .option("cloudFiles.format", "csv") \
      .load(folder_path)
  return df



# COMMAND ----------


