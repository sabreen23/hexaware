# Databricks notebook source
# MAGIC %run "/Workspace/Users/sabreen23@gmail.com/Hexware Training/Day5/includes"

# COMMAND ----------

df=spark.read.csv(f"{input}",header=True,inferSchema=True)

# COMMAND ----------

df1=add_ingestion(df)

# COMMAND ----------


df1.columns
 

# COMMAND ----------

new_col=['name', 'country', 'industry', 'net_worth_in_billions', 'company','ingestion_date']

# COMMAND ----------

df2=df1.toDF(*new_col)

# COMMAND ----------

df2.write.mode("overwrite").save(f"{output}sabreen/richest")

# COMMAND ----------

dbutils.widgets.text("environment"," ")
w=dbutils.widgets.get("environment")

# COMMAND ----------

df3=df2.withColumn("environment",lit(w))

# COMMAND ----------

df3.write.mode("overwrite").option("mergeSchema", "true").save(f"{output}sabreen/richest")

# COMMAND ----------

# MAGIC %sql
# MAGIC create schema if not exists ssdatabricksdemo.bronze;
# MAGIC use bronze

# COMMAND ----------

df3.write.mode("overwrite").option("mergeSchema","true").saveAsTable("richest_bronze")

# COMMAND ----------


 
