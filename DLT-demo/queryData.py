# Databricks notebook source
# MAGIC %sql
# MAGIC 
# MAGIC select count(*) from dltstream.raw_table

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC drop database dltstream

# COMMAND ----------

# MAGIC %sql
# MAGIC drop table dltstream.raw_table

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC select * from dltstream.cdcData

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC select * from dltstream.customersv1 
# MAGIC where customer_id between 710 and 715
# MAGIC --where customer_id=680 order by __end_at asc

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC select * from dltstream.customersv1 order by customer_id

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC select * from dltstream.customers_scd_t1 
# MAGIC where customer_id between 710 and 715
# MAGIC order by customer_id

# COMMAND ----------

storage_path

# COMMAND ----------



# COMMAND ----------

# MAGIC %sql 
# MAGIC CREATE TABLE IF NOT EXISTS dltstream.demo_cdc_dlt_system_event_log_raw using delta LOCATION '/mnt/dlt-container/tables/delta-table/system/events';
# MAGIC select * from dltstream.demo_cdc_dlt_system_event_log_raw;

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC select * from dltstream.customer_bronze

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC select * from dltstream.customer_silver_scd_type_2 where customer_id=849

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC select * from dltstream.customer_gold_scd_type_2 where customer_id=849

# COMMAND ----------


