# Databricks notebook source
# MAGIC %run "/Workspace/Users/naval@thedatamaster.in/Hexware Training/Day5/includes"

# COMMAND ----------

users_schema=StructType([StructField("Year",IntegerType()),
                         StructField("first_name",StringType()),
                         StructField("County",StringType()),
                         StructField("Gender",StringType()),
                         StructField("Count",IntegerType())
])

# COMMAND ----------

df=spark.read.csv(f"{input}Baby_Names.csv")

# COMMAND ----------

df=spark.read.csv(f"{input}Baby_Names.csv",header=True,schema=users_schema)

# COMMAND ----------

display(df)

# COMMAND ----------

df.filter("Year=2007").display()

# COMMAND ----------

df.count()

# COMMAND ----------

df.groupBy("Year").count().orderBy("Year").display()

# COMMAND ----------

df.groupBy("Year").count().orderBy("Year").explain()

# COMMAND ----------

df_new=spark.read.table("datamaster.bronze.baby_name")

# COMMAND ----------

df_new.filter("Year=2008").display()

# COMMAND ----------

df_new.groupBy("Year").count().orderBy("Year").display()
