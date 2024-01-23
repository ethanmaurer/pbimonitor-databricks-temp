-- Databricks notebook source
-- MAGIC %md
-- MAGIC #Capacities SQL
-- MAGIC Blank Notebook for writing SQL statements on Activity tables

-- COMMAND ----------

CREATE OR REPLACE TEMP VIEW capacities
AS (
  SELECT *
  FROM transformed.capacities
  WHERE processedTimeStamp = (
    SELECT MAX(processedTimeStamp)
    FROM transformed.capacities
  )
)

-- COMMAND ----------

CREATE OR REPLACE TEMP VIEW users
AS (
  SELECT *
  FROM transformed.capacities_users
  WHERE processedTimeStamp = (
    SELECT MAX(processedTimeStamp)
    FROM transformed.capacities
  )
)

-- COMMAND ----------


