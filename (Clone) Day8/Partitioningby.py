# Databricks notebook source
# MAGIC %run "/Workspace/Users/naval@thedatamaster.in/Hexware Training/Day5/includes"

# COMMAND ----------



# COMMAND ----------

users_schema=StructType([StructField("Year",IntegerType()),
                         StructField("first_name",StringType()),
                         StructField("County",StringType()),
                         StructField("Gender",StringType()),
                         StructField("Count",IntegerType())
])

# COMMAND ----------

df=spark.read.csv(f"{input}Baby_Names.csv",header=True,schema=users_schema)

# COMMAND ----------

display(df)

# COMMAND ----------

df.count()

# COMMAND ----------

df.groupBy("Year").count().orderBy("Year").display()

# COMMAND ----------

df.write.saveAsTable("datamaster.bronze.baby_name")

# COMMAND ----------

output

# COMMAND ----------

df.write.option("path","dbfs:/mnt/hexawaredatabricks/raw/output_files/naval/baby_names").saveAsTable("datamaster.bronze.baby_name_ext")

# COMMAND ----------

df.write.save("dbfs:/mnt/hexawaredatabricks/raw/output_files/------/baby_names")

# COMMAND ----------

df1=df.filter("Year=2007")

# COMMAND ----------

df1.write.partitionBy("Year").save("dbfs:/mnt/hexawaredatabricks/raw/output_files/------/baby_names_year")

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from delta.`dbfs:/mnt/hexawaredatabricks/raw/output_files/naval/baby_names_year` where Year=2007

# COMMAND ----------

df.write.partitionBy("Year","Gender").save("dbfs:/mnt/hexawaredatabricks/raw/output_files/------/baby_names_year_gender")

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from datamaster.bronze.baby_name where Year=2007 and Gender='F'

# COMMAND ----------

# MAGIC %sql
# MAGIC describe detail datamaster.bronze.baby_name

# COMMAND ----------

# MAGIC %sql
# MAGIC desc extended datamaster.bronze.baby_name
