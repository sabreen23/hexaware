-- Databricks notebook source
hexaware.dev.employeehexaware.dev.employee%python
print("Hi Team")

-- COMMAND ----------

-- MAGIC %python
-- MAGIC print("hii Ankush")

-- COMMAND ----------

-- MAGIC %python
-- MAGIC "Shubham"

-- COMMAND ----------

-- MAGIC %python
-- MAGIC print("Haritha")

-- COMMAND ----------

-- MAGIC %python
-- MAGIC "Aashi"

-- COMMAND ----------

-- MAGIC %python
-- MAGIC Print ( "Hi Team,Have a nice day")

-- COMMAND ----------



-- COMMAND ----------

-- MAGIC %python
-- MAGIC Stephen

-- COMMAND ----------



-- COMMAND ----------

-- MAGIC %python
-- MAGIC print("Hi Team this is janaki")

-- COMMAND ----------

Program SQL , UI
Catalog
schema
table ,view 

Data access control
Data lineage
data discover

Data auditing

-- COMMAND ----------

use catalog hexaware;

-- COMMAND ----------

create schema dev;

-- COMMAND ----------

use dev;
create table employee(id int, name string)

-- COMMAND ----------

insert into employee values(1,'Naval')

-- COMMAND ----------

-- DBTITLE 1,Janaki
create schema Mith

-- COMMAND ----------

create schema Shubham

-- COMMAND ----------



-- COMMAND ----------

GRANT USAGE on schema dev to `jdeepa15@gmail.com`

-- COMMAND ----------

GRANT USAGE on schema dev to `account users`

-- COMMAND ----------

  GRANT <privilege-type> ON <securable-type> <securable-name> TO <principal>


-- COMMAND ----------

select * from hexaware.dev.employee

-- COMMAND ----------

use catalog hexaware;
use dev

-- COMMAND ----------

describe history employee

-- COMMAND ----------

create view employee_id_count as select id, count(id) as count from employee group by id

-- COMMAND ----------


