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

df_address = spark.read.csv ("/mnt/salesdata/SalesLT.Address/*", header=True)

from pyspark.sql import functions as F
count_of_country_region = df_address.select("CountryRegion").distinct().count()
df_address = df_address.repartition(count_of_country_region)
print(f" the input partition: {df_address.rdd.getNumPartitions()}")

# COMMAND ----------

deltaPath = "/mnt/databricksplayground/replace_where_example/deltatable/saleslt.address/"
df_address.write.format("delta").option("replaceWhere", "CountryRegion = 'Canada'").mode("overwrite").save(deltaPath)

# COMMAND ----------

display(df_address)

# COMMAND ----------


