# Databricks notebook source
# MAGIC %md
# MAGIC reading the data

# COMMAND ----------

df_address = spark.read.csv ("/mnt/salesdata/SalesLT.Address/*", header=True)

from pyspark.sql import functions as F
count_of_country_region = df_address.select("CountryRegion").distinct().count()
df_address = df_address.repartition(count_of_country_region)
print(f" the input partition: {df_address.rdd.getNumPartitions()}")

deltaPath = "/mnt/databricksplayground/autoloader_streaming/deltatable/saleslt.address/"

# df_address.write.partitionBy("CountryRegion").format("delta").mode("overwrite").save(deltaPath)

# COMMAND ----------

schema = df_address.schema

# COMMAND ----------



# COMMAND ----------

# getting the secrets from the key vault scoped databricks secrets
subscriptionId = dbutils.secrets.get(scope="sapaakv-secret-scope", key = "subscriptionID")
tenantId = dbutils.secrets.get(scope="sapaakv-secret-scope", key = "tenantId")
clientId = dbutils.secrets.get(scope="sapaakv-secret-scope", key = "clientId")
clientSecret = dbutils.secrets.get(scope="sapaakv-secret-scope", key = "clientSecret")
qu_sas_key = dbutils.secrets.get(scope="sapaakv-secret-scope", key = "qusaskey")

cloudfile = {
  "cloudfiles.subscriptionId": subscriptionId,
  "cloudfiles.connectionString": qu_sas_key,
  "cloudfiles.format": "csv",
  "cloudfiles.tenantId": tenantId,
  "cloudfiles.clientId": clientId,
  "cloudfiles.clientSecret": clientSecret,
  "cloudfiles.resourceGroup": "RGSam",
  "cloudFiles.useNotifications": True
  
}

# COMMAND ----------

cloudfile


# COMMAND ----------

checkpoint_path = '/mnt/databricksplayground/autoloader_streaming/checkpoint/saleslt.address/_checkpoints'
deltaPath = "/mnt/databricksplayground/autoloader_streaming/deltatable/saleslt.address/"


from pyspark.sql import functions as F
# count_of_country_region = df_address.select("CountryRegion").distinct().count()
# df_address = df_address.repartition(count_of_country_region)
# print(f" the input partition: {df_address.rdd.getNumPartitions()}")

df = spark.readStream.format("cloudFiles").options(**cloudfile).schema(schema).load("/mnt/salesdata/SalesLT.Address/")

# COMMAND ----------

df.writeStream.format("delta").option("checkpointLocation", checkpoint_path).start(deltaPath)


# COMMAND ----------

table_delta_file_location = "/mnt/databricksplayground/autoloader_streaming/deltatable/saleslt.address/"
table_full_name = "saleslt_address_autoloader_streaming"
sqltext = f"CREATE TABLE IF NOT EXISTS {table_full_name} USING DELTA LOCATION '{table_delta_file_location}'"
spark.sql(sqltext)

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC select count(*) from saleslt_address_autoloader_streaming

# COMMAND ----------


