-- Databricks notebook source
-- MAGIC %md
-- MAGIC #MSGraph SQL
-- MAGIC Blank Notebook for writing SQL statements on Graph tables

-- COMMAND ----------

-- MAGIC %python
-- MAGIC
-- MAGIC tables = ["securityGroups", "subscribedSKUs", "users", "usersInSecurityGroups"]
-- MAGIC
-- MAGIC for table in tables:
-- MAGIC     spark.sql(f'''
-- MAGIC               CREATE OR REPLACE TEMP VIEW {table}
-- MAGIC               AS (
-- MAGIC                   SELECT *
-- MAGIC                   FROM transformed.msgraph_{table}
-- MAGIC                   WHERE processedTimeStamp = (
-- MAGIC                       SELECT MAX (processedTimeStamp)
-- MAGIC                       FROM transformed.msgraph_{table}
-- MAGIC                   )
-- MAGIC               )
-- MAGIC               ''')

-- COMMAND ----------


