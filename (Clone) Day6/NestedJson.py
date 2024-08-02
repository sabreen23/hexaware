# Databricks notebook source
# MAGIC %run "/Workspace/Users/sabreen23@gmail.com/Hexware Training/Day5/includes"

# COMMAND ----------

input

# COMMAND ----------

spark.read.json("dbfs:/mnt/hexawaredatabricks/raw/input_files/adobe_sample.json",multiLine=True).display()

# COMMAND ----------

df=spark.read.json("dbfs:/mnt/hexawaredatabricks/raw/input_files/adobe_sample.json",multiLine=True)

# COMMAND ----------

df1=df.withColumn("batters",explode("batters.batter"))\
.withColumn("batters_id",col("batters.id"))\
.withColumn("batters_type",col("batters.type"))\
.drop("batters")\
.withColumn("topping",explode("topping"))\
.withColumn("topping_id",col("topping.id"))\
.withColumn("topping_type",col("topping.type"))\
.drop("topping")

# COMMAND ----------



# COMMAND ----------

display(df1)

# COMMAND ----------

df1.createOrReplaceTempView("adobe")

# COMMAND ----------

df1.filter("batters_type='Chocolate'").display()

# COMMAND ----------

df1.where("batters_type='Chocolate' and topping_id=5001").display()

# COMMAND ----------

df1.sort(col("topping_id").desc()).display()

# COMMAND ----------

df1.sort(col("topping_id").desc(),"batters_type").display()

# COMMAND ----------

df1.where("batters_type='Chocolate'").display()
