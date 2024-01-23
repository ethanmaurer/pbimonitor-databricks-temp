-- Databricks notebook source
-- MAGIC %md
-- MAGIC #Activity Events SQL
-- MAGIC Blank Notebook for writing SQL statements on Activity tables

-- COMMAND ----------

CREATE OR REPLACE TEMP VIEW a
AS(
  select *
  From transformed.activityevents
)

-- COMMAND ----------

CREATE OR REPLACE TEMP VIEW facts
AS(
  select *
  From transformed.activityevents_fact
)
