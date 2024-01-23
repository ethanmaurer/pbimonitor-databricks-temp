-- Databricks notebook source
-- MAGIC %md
-- MAGIC #Tenant Settings SQL
-- MAGIC Blank Notebook for writing SQL statements on Activity tables

-- COMMAND ----------

CREATE OR REPLACE TEMP VIEW settings
AS (
  SELECT *
  FROM transformed.tenantSettings
  WHERE processedTimeStamp = (
    SELECT MAX(processedTimeStamp)
    FROM transformed.tenantSettings
  )
)

-- COMMAND ----------


