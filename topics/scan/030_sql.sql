-- Databricks notebook source
-- MAGIC %md
-- MAGIC #Activity Events SQL
-- MAGIC Blank Notebook for writing SQL statements on Activity tables

-- COMMAND ----------

CREATE OR REPLACE TEMP VIEW workspaces
AS (
  SELECT *
  FROM transformed.scan_result_workspaces
)

-- COMMAND ----------


