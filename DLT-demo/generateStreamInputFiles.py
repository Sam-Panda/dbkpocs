# Databricks notebook source
# MAGIC %pip install names

# COMMAND ----------

import os
import datetime
import csv
import random
import time
import names
import random
import pandas as pd

# declare some variables here 
number_of_files_to_be_generated = 2
number_of_seconds_to_be_waited = 1

def create_random_names():
    name = names.get_full_name()


def create_file():
  now = datetime.datetime.now()
  file_name = "MyFirstBlob.csv"
  mount_point_name = "/mnt/dlt-container"
  # Format the date and time as a string
  date_time_string = now.strftime("%Y-%m-%d_%H-%M-%S")
  new_file_name = f"{mount_point_name}/streamcsvfiles/{file_name}_{date_time_string}.csv"

  # Set up the headers and data
  headers = ['Name', 'Age', 'Country']
  data = []
  for i in range(10):
      name = create_random_names()
      age = random.randint(10, 60)
      country = random.choice(['USA', 'UK', 'Australia', 'Canada', 'India', "dummy"])
      data.append([name, age, country])
  df = pd.DataFrame(data, columns=headers)
  #df.coalesce(1).write.csv(new_file_name, header=True)
  # writing the files into the mount point
  dbutils.fs.put( new_file_name, df.to_csv(index=False), True)
    
    

for i in range(number_of_files_to_be_generated):
      create_file()
      time.sleep(number_of_seconds_to_be_waited)
      
        
        

# COMMAND ----------


