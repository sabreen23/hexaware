# Databricks notebook source
# MAGIC %fs ls

# COMMAND ----------

dbutils.fs.mount(
  source = "wasbs://raw@datamasterdatabricks.blob.core.windows.net",
  mount_point = "/mnt/datamasterdatabricks/raw",
  extra_configs = {"fs.azure.account.key.datamasterdatabricks.blob.core.windows.net":"accessey"})
