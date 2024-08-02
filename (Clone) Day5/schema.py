# Databricks notebook source
# MAGIC %run "/Workspace/Users/sabreen23@gmail.com/Hexware Training/Day5/includes"

# COMMAND ----------

df=spark.read.csv(input,header=True)

# COMMAND ----------

from pyspark.sql.types import *

# COMMAND ----------

schema=StructType([StructField("Name",StringType()),
                StructField("Country",StringType()),
                StructField("Industry",StringType()),
                StructField("Net_Worth_in_billions)",DoubleType()),
                 StructField("Company",StringType()),
]
)

# COMMAND ----------

df=spark.read.schema(schema).csv(input,header=True)
