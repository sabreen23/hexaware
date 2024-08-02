# Databricks notebook source
# MAGIC %sql
# MAGIC create schema if not exists ssdatabricksdemo.silver

# COMMAND ----------

# MAGIC %sql
# MAGIC use ssdatabricksdemo.silver;
# MAGIC Create or replace table silver.richest_silver as
# MAGIC select name, country, industry, net_worth_in_billions, company from ssdatabricksdemo.bronze.richest_bronze

# COMMAND ----------

# MAGIC %sql
# MAGIC create schema if not exists ssdatabricksdemo.gold;
# MAGIC use gold

# COMMAND ----------

# MAGIC %sql
# MAGIC create or replace table gold.country_count as  
# MAGIC select country, count(country) as count from ssdatabricksdemo.silver.richest_silver group by country order by count desc
