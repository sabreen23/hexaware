# Databricks notebook source
(spark
.readStream
.format("cloudFiles")
.option("cloudFiles.format","csv")
.option("cloudFiles.inferColumnTypes",True)
.option("cloudFiles.schemaLocation","dbfs:/mnt/hexawaredatabricks/raw/schemalocation/sabreen/autoloader")
.load("dbfs:/mnt/hexawaredatabricks/raw/stream_in/")
.writeStream
.option("checkpointLocation","dbfs:/mnt/hexawaredatabricks/raw/checkpoint/sabreen/autoloader")
.trigger(once=True)
.table("ssdatabricksdemo.bronze.autoloader")
)

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from ssdatabricksdemo.bronze.autoloader

# COMMAND ----------

(spark
.readStream
.format("cloudFiles")
.option("cloudFiles.format","csv")
.option("cloudFiles.inferColumnTypes",True)
.option("cloudFiles.schemaEvolutionMode","addNewColumns")
.option("cloudFiles.schemaLocation","dbfs:/mnt/hexawaredatabricks/raw/schemalocation/sabreen/autoloader")
.load("dbfs:/mnt/hexawaredatabricks/raw/stream_in/")
.writeStream
.option("checkpointLocation","dbfs:/mnt/hexawaredatabricks/raw/checkpoint/sabreen/autoloader")
.option("mergeSchema",True)
.table("ssdatabricksdemo.bronze.autoloader")
)
