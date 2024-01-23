-- Databricks notebook source
-- MAGIC %md
-- MAGIC #PBIPipelines SQL
-- MAGIC Blank Notebook for writing SQL statements on PBIPipelines tables

-- COMMAND ----------

CREATE OR REPLACE TEMP VIEW pipelines
AS (
  SELECT *
  FROM transformed.pbipipelines
  WHERE processedTimeStamp = (
    SELECT MAX(processedTimeStamp)
    FROM transformed.pbipipelines
  )
)

-- COMMAND ----------

CREATE OR REPLACE TEMP VIEW users
AS (  
  SELECT *
  FROM transformed.pbipipelines_users
  WHERE processedTimeStamp = (
    SELECT MAX(processedTimeStamp)
    FROM transformed.pbipipelines
  )
)

-- COMMAND ----------


