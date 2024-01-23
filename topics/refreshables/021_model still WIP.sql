-- Databricks notebook source
-- MAGIC %md
-- MAGIC # Refreshables Model
-- MAGIC This notebook reads the table from the 020_load notebook and creates dimension tables 
-- MAGIC
-- MAGIC Not completed

-- COMMAND ----------

-- MAGIC %run ./010_dependencies

-- COMMAND ----------

CREATE OR REPLACE TEMP VIEW refreshables
AS (
  SELECT *
  FROM transformed.refreshables
)

-- COMMAND ----------

SELECT *
FROM refreshables

-- COMMAND ----------


