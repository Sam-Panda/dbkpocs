# Databricks notebook source
# MAGIC %sql 
# MAGIC CREATE TABLE IF NOT EXISTS dltstream.demo_cdc_dlt_system_event_log_raw using delta LOCATION '/mnt/dlt-container/tables/delta-table/system/events';
# MAGIC select * from dltstream.demo_cdc_dlt_system_event_log_raw;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT 
# MAGIC        id,
# MAGIC        timestamp,
# MAGIC        sequence,
# MAGIC        event_type,
# MAGIC        message,
# MAGIC        level, 
# MAGIC        details
# MAGIC   FROM dltstream.demo_cdc_dlt_system_event_log_raw
# MAGIC  ORDER BY timestamp ASC
# MAGIC ;  

# COMMAND ----------

# MAGIC %sql
# MAGIC -- List of output datasets by type and the most recent change
# MAGIC create or replace temp view cdc_dlt_expectations as (
# MAGIC   SELECT 
# MAGIC     id,
# MAGIC     timestamp,
# MAGIC     details:flow_progress.metrics.num_output_rows as output_records,
# MAGIC     details:flow_progress.data_quality.dropped_records,
# MAGIC     details:flow_progress.status as status_update,
# MAGIC     explode(from_json(details:flow_progress.data_quality.expectations
# MAGIC              ,'array<struct<dataset: string, failed_records: bigint, name: string, passed_records: bigint>>')) expectations
# MAGIC   FROM dltstream.demo_cdc_dlt_system_event_log_raw
# MAGIC   where details:flow_progress.status='COMPLETED' and details:flow_progress.data_quality.expectations is not null
# MAGIC   ORDER BY timestamp);
# MAGIC select * from cdc_dlt_expectations

# COMMAND ----------


