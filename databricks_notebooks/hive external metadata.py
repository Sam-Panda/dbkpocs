# Databricks notebook source
# MAGIC %scala
# MAGIC 
# MAGIC import com.typesafe.config.ConfigFactory
# MAGIC val path = ConfigFactory.load().getString("java.io.tmpdir")
# MAGIC 
# MAGIC println(s"\nHive JARs are downloaded to the path: $path \n")

# COMMAND ----------

# MAGIC %sh cp -r /local_disk0/tmp /dbfs/hive_metastore_jar

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC create database testdb

# COMMAND ----------


