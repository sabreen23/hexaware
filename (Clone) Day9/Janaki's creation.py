# Databricks notebook source
# MAGIC %sql
# MAGIC insert into hexaware.dev.employee values(1,'janakideepa')

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from hexaware.dev.employee

# COMMAND ----------

# MAGIC %sql describe extended hexaware.dev.employee

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from hexaware.dev.employee

# COMMAND ----------

# MAGIC %sql
# MAGIC update hexaware.dev.employee  set  id =5 where name='janakideepa'
