# Databricks notebook source
spark.conf.get("spark.sql.autoBroadcastJoinThreshold").ca

# COMMAND ----------

df1 = spark.range(1000)
df2 = spark.range(1000)
df3 = spark.range(1000)

# COMMAND ----------

joined_df = df1.join(df2, df1.id==df2.id)

# COMMAND ----------

joined_df.explain()

# COMMAND ----------


