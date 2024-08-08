# Databricks notebook source
# MAGIC %run "/Workspace/Users/naval@thedatamaster.in/Hexware Training/Day5/includes"

# COMMAND ----------

input

# COMMAND ----------

df_summary=spark.read.csv(f"{input}linkedIn_dataset/job_summary.csv",header=True)

# COMMAND ----------

df_summary.display()

# COMMAND ----------

df_summary.count()

# COMMAND ----------

df_summary.write.saveAsTable("bronze.job_summary")

# COMMAND ----------

df_new=spark.read.table("bronze.job_summary")

# COMMAND ----------

df_new.count()

# COMMAND ----------

df_job_skills=spark.read.csv(f"{input}linkedIn_dataset/job_skills.csv",header=True)

# COMMAND ----------

df_job_skills.display()

# COMMAND ----------


