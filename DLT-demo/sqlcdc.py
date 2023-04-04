# Databricks notebook source
import dlt
from pyspark.sql.functions import col, expr
folder_path = "/mnt/dlt-container/CDCdata/sqlserver"

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ## Read the CDC data using the Delta Live Table Streaming capabilities. ( equivalent to Autoloader )

# COMMAND ----------



@dlt.table(
  name="customer_bronze",
  comment = "The incrmenetal customer change data ingested from the data lake",
  path = "/mnt/dlt-container/tables/delta-table/tables/_customer_bronze",
  table_properties={
    "quality": "bronze"
  }
)
def customer_bronze():
  df = spark.readStream.format("cloudFiles") \
      .option("cloudFiles.format", "csv") \
      .load(folder_path)
  return df





# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ## create the silver layer, here we are going to have 2 tables
# MAGIC    - one table with SCD type 1
# MAGIC    - another table with SCD type 2

# COMMAND ----------

'''

Use the apply_changes() function in the Python API to use Delta Live Tables CDC functionality. The Delta Live Tables Python CDC interface also provides the create_streaming_live_table() function. You can use this function to create the target table required by the apply_changes() function.

'''


dlt.create_streaming_live_table(
  name = "customer_silver_scd_type_1",
  comment = " this is the silver data - SCD type 1",
  path = "/mnt/dlt-container/tables/delta-table/tables/customer_silver_scd_type_1",
    table_properties={
    "quality": "silver"
  }
)
  
dlt.apply_changes(
  target = "customer_silver_scd_type_1",
  source = "customer_bronze",
  keys = ["customer_id"],
  sequence_by = col("ModifiedDate"),
  apply_as_deletes = expr("operation = 'DELETE'"),
  except_column_list = ["operation"],
  stored_as_scd_type = "1"
)


dlt.create_streaming_live_table(
  name = "customer_silver_scd_type_2",
  comment = " this is the silver data - SCD type 2",
  path = "/mnt/dlt-container/tables/delta-table/tables/customer_silver_scd_type_2",
    table_properties={
    "quality": "silver"
  }
)

dlt.apply_changes(
  target = "customer_silver_scd_type_2",
  source = "customer_bronze",
  keys = ["customer_id"],
  sequence_by = col("ModifiedDate"),
  apply_as_deletes = expr("operation = 'DELETE'"),
  except_column_list = ["operation"],
  stored_as_scd_type = "2"
)


# COMMAND ----------

# MAGIC %md
# MAGIC ## create the gold layer
# MAGIC  - from the SCD type 2 table we will discard the old records
# MAGIC  - we will create a view, and create a table from the view. 
# MAGIC  

# COMMAND ----------

@dlt.table(name="vw_customer_silver_clean_scd_type_2",
  comment="Cleansed silver customer view (i.e. what will become gold)")

@dlt.expect_or_drop("recent_record", "__END_AT IS NULL")


def vw_customer_silver_clean_scd_type_2():
  df = dlt.read("customer_silver_scd_type_2") \
        .select("customer_id", "first_name", "last_name", "email", "city", "createdDate", "ModifiedDate", "__START_AT", "__END_AT")
  return df




@dlt.table(
  name = "customer_gold_scd_type_2",
  comment = " this is the gold data after cleansing from SCD type 2 table (customer_silver_scd_type_2)",
  path = "/mnt/dlt-container/tables/delta-table/tables/customer_gold_scd_type_2",
    table_properties={
    "quality": "gold"
  }
)
def customer_gold_scd_type_2():
  return dlt.read("vw_customer_silver_clean_scd_type_2")
  
