# Databricks notebook source
# MAGIC %md
# MAGIC 
# MAGIC # GDPR implementation using the Pseudonymization  and data masking
# MAGIC 
# MAGIC > Prerequisite:
# MAGIC > - moun point already created to an adls gen2 file system with the name : /mnt/gdpr
# MAGIC > - Download an open source dataset from Kaggle to have the user data : https://www.kaggle.com/omercolakoglu/10m-rows-fake-turkish-names-and-address-dataset  (The data is in Excel format, change the data into csv and load it in the delta table)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Store the raw data into a delta table [gdpr.raw_customer_data] with a Pseudo Key.

# COMMAND ----------

# MAGIC %md
# MAGIC ### read the raw data

# COMMAND ----------

dbutils.fs.ls ("/mnt/gdpr/rawfile")

# COMMAND ----------

df_csv = spark.read.csv("dbfs:/mnt/gdpr/rawfile/Customers_1M_Rows.csv", header=True)

# COMMAND ----------

# MAGIC %md 
# MAGIC ### Pseduonymize the email address
# MAGIC > - Here we are considering that email address is unquiley identify the customers, and hence Pseudo key will uniquely identify the customers as well. 

# COMMAND ----------

import pyspark.sql.functions as F
df = df_csv.withColumn("customer_pseudo_id", F.sha2(F.col("email"), 256))


# COMMAND ----------

# MAGIC %md
# MAGIC ### write the customer table [gdpr.raw_customer_data] into the delta lake

# COMMAND ----------


spark.sql('''create schema if not exists gdpr ''')
# drop the table if it exists
dbutils.fs.rm("/mnt/gdpr/deltalake/gdpr/raw_customer_data", recurse=True)
spark.sql('''drop table if exists gdpr.raw_customer_data''')

# write the dataframe as delta 
df.write.format("delta").mode("overwrite").save("/mnt/gdpr/deltalake/gdpr/raw_customer_data")
# create the delta table
spark.sql('''create table gdpr.raw_customer_data using delta location "/mnt/gdpr/deltalake/gdpr/raw_customer_data"''')

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC select ID, Email, customer_pseudo_id from gdpr.raw_customer_data

# COMMAND ----------

# MAGIC %md
# MAGIC ## Data Masking

# COMMAND ----------

# MAGIC %md
# MAGIC ### Generating the Cryptographic Key
# MAGIC 
# MAGIC > - Run these below commands in the local environment to setup the key. The key can be generated any system. 
# MAGIC > - First upgrade the pip to the latest version using the command: python -m pip install --upgrade pip 
# MAGIC > - pip install fernet
# MAGIC > - pip install cryptography
# MAGIC > - Generate a key in the local environment 
# MAGIC 
# MAGIC ```
# MAGIC from cryptography.fernet import Fernet
# MAGIC key = Fernet.generate_key()
# MAGIC ```

# COMMAND ----------

# creating the user defined function to create the encryption key 
def generate_encrypt_key():
    from cryptography.fernet import Fernet
    key = Fernet.generate_key()
    return key.decode("utf-8")
spark.udf.register("generate_key_using_Fernet", generate_encrypt_key)

# COMMAND ----------

# MAGIC %md
# MAGIC ### gdpr.Encryption_key table keep the mapping between the customer ID and encryption keys

# COMMAND ----------

import pyspark.sql.functions as F
from pyspark.sql.types import StringType
generate_key_using_Fernet = udf(generate_encrypt_key, StringType())
df_distinct_record = spark.sql('''select distinct ID from gdpr.raw_customer_data''')
df_distinct_record = df_distinct_record.withColumn("encryption_key", F.lit(generate_key_using_Fernet()))

dbutils.fs.rm("/mnt/gdpr/deltalake/gdpr/encryption_keys", recurse=True)
spark.sql('''drop table if exists gdpr.encryption_keys''')
df_distinct_record.write.format("delta").mode("overwrite").save("/mnt/gdpr/deltalake/gdpr/encryption_keys")
spark.sql('''create table gdpr.encryption_keys using delta location "/mnt/gdpr/deltalake/gdpr/encryption_keys"''')

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from gdpr.encryption_keys

# COMMAND ----------

# MAGIC %md
# MAGIC ### create the spark UDF to encrypt and decrypt the column.

# COMMAND ----------

# Define Encrypt User Defined Function 
def encrypt_val(clear_text,MASTER_KEY):
    from cryptography.fernet import Fernet
    f = Fernet(MASTER_KEY)
    clear_text_b=bytes(clear_text, 'utf-8')
    cipher_text = f.encrypt(clear_text_b)
    cipher_text = str(cipher_text.decode('ascii'))
    return cipher_text

# Define decrypt user defined function 
def decrypt_val(cipher_text,MASTER_KEY):
    from cryptography.fernet import Fernet
    f = Fernet(MASTER_KEY)
    clear_val=f.decrypt(cipher_text.encode()).decode()
    return clear_val
spark.udf.register("decrypt_val", decrypt_val)

# COMMAND ----------

# MAGIC %md 
# MAGIC ### Encryption
# MAGIC 
# MAGIC - We are going to encrypt the column Email.

# COMMAND ----------

from pyspark.sql.functions import udf, lit, md5, col
from pyspark.sql.types import StringType
 
# Register UDF's
encrypt = udf(encrypt_val, StringType())
decrypt = udf(decrypt_val, StringType())
 
 
# Encrypt the data 
df = spark.sql('''select a.*,e.encryption_key from gdpr.raw_customer_data as a 
inner join gdpr.encryption_keys as e on e.ID=a.ID''')
encrypted = df.withColumn("EMAIL", encrypt("EMAIL", col("encryption_Key"))).drop("encryption_Key")
# display(encrypted.limit(10))
 
#Save encrypted data 
encrypted.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable("gdpr.raw_customer_data")

# COMMAND ----------

# MAGIC %md
# MAGIC ### masked data

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC select ID, Email, customer_pseudo_id from gdpr.raw_customer_data

# COMMAND ----------


