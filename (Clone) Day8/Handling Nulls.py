# Databricks notebook source
schema=["name", "subject", "Marks", "Attendance"]

student_data=[("John","Math", 90, 80),("Michael", "Science", 70, None), ("David", "History", 50,40), ("Kelvin", "Computer", 40,None ),("Paul", "GEO", None, None), (None,None,  None, None),("John","Math", 90, 80),("John","Math", 90, 80),(None,None,  None, None),(None,None,  None, None),(None,None,  None, None),("Michael", "Science", 70, None),(None, "Science", 90, None),(" ", "NAN", 55, None)]

df=spark.createDataFrame(data=student_data, schema=schema)
display(df)
df.printSchema()

# COMMAND ----------

df.distinct().display()

# COMMAND ----------

df1=df.dropDuplicates()

# COMMAND ----------

https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.DataFrameNaFunctions.html?highlight=na%20functions#pyspark.sql.DataFrameNaFunctions

# COMMAND ----------

Handle nulls
- Drop nulls-- dropna or na.drop
- fill nulls.--- fillna or na.fill
- replace ----- replace

# COMMAND ----------

help(df.dropna)

# COMMAND ----------

df1.display()

# COMMAND ----------

df2=df1.dropna("all")

# COMMAND ----------

df2.display()

# COMMAND ----------

df3=df2.na.drop(subset="name")

# COMMAND ----------

help(df.fillna)

# COMMAND ----------

df3.fillna(34,subset="Attendance").display()

# COMMAND ----------

df4=df3.fillna({"Marks":39,"Attendance":34})

# COMMAND ----------

df4.replace("NAN","IT",subset="subject").replace(" ","Robert").display()

# COMMAND ----------


