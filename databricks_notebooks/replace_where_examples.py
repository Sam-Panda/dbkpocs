# Databricks notebook source
dbutils.fs.mounts()

# COMMAND ----------

df_address = spark.read.csv ("/mnt/salesdata/SalesLT.Address/*", header=True)

from pyspark.sql import functions as F
count_of_country_region = df_address.select("CountryRegion").distinct().count()
df_address = df_address.repartition(count_of_country_region)
print(f" the input partition: {df_address.rdd.getNumPartitions()}")

deltaPath = "/mnt/databricksplayground/replace_where_example/deltatable/saleslt.address/"

df_address.write.partitionBy("CountryRegion").format("delta").mode("overwrite").save(deltaPath)

# COMMAND ----------

table_delta_file_location = "/mnt/databricksplayground/replace_where_example/deltatable/saleslt.address/"
table_full_name = "saleslt_address"
sqltext = f"CREATE TABLE IF NOT EXISTS {table_full_name} USING DELTA LOCATION '{table_delta_file_location}'"
spark.sql(sqltext)

# COMMAND ----------

spark.conf.set("spark.databricks.delta.retentionDurationCheck", False)

# COMMAND ----------

# MAGIC %sql
# MAGIC VACUUM saleslt_address RETAIN 10 HOURS

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC select * from saleslt_address where CountryRegion="India"

# COMMAND ----------

# vaccum example with hours
df_address = spark.read.csv ("/mnt/salesdata/SalesLT.Address/*", header=True)

from pyspark.sql import functions as F
count_of_country_region = df_address.select("CountryRegion").distinct().count()
df_address = df_address.repartition(count_of_country_region)
print(f" the input partition: {df_address.rdd.getNumPartitions()}")

deltaPath = "/mnt/databricksplayground/replace_where_example/deltatable/saleslt.address/"
df_address = df_address.where(F.col("CountryRegion") == "India")
df_address.write.format("delta").option("replaceWhere", "CountryRegion = 'India'").mode("overwrite").save(deltaPath)



# COMMAND ----------



# COMMAND ----------



# COMMAND ----------

display(df_address)

# COMMAND ----------


