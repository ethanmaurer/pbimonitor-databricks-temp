-- Databricks notebook source
-- MAGIC %md
-- MAGIC # Capacity Model
-- MAGIC This notebook reads the table from the 020 and 021 load notebooks and creates dimension tables 

-- COMMAND ----------

-- MAGIC %run ./010_dependencies

-- COMMAND ----------

CREATE OR REPLACE TEMP VIEW capacities
AS (
  SELECT *
  FROM transformed.capacities
)

-- COMMAND ----------

CREATE OR REPLACE TEMP VIEW users
AS (
  SELECT *
  FROM transformed.capacities_users
)

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Dimensions

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Capacities

-- COMMAND ----------

CREATE OR REPLACE TEMP VIEW dimCapacities
AS (
  SELECT * EXCEPT(admins, capacityUserAccessRight, users)
  FROM capacities
)

-- COMMAND ----------

SELECT *
FROM dimCapacities

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ###Capacity Users

-- COMMAND ----------

CREATE OR REPLACE TEMP VIEW dimCapacityUsers
AS (
  SELECT *
  FROM users
)

-- COMMAND ----------

SELECT *
FROM dimCapacityUsers

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Overwrite and Optimize Tables

-- COMMAND ----------

-- MAGIC %python
-- MAGIC writePath = '/mnt/data-warehouse/apiData/transformed/capacities_dimCapacities'  ### ADD a version number to the folder path
-- MAGIC overwriteDeltaTable(toDf('dimCapacities').drop('processedDate', 'processedTimestamp'), writePath)

-- COMMAND ----------

-- MAGIC %python
-- MAGIC sql_optimize_vacuum('transformed', 'capacities_dimCapacities', writePath)

-- COMMAND ----------

-- MAGIC %python
-- MAGIC writePath = '/mnt/data-warehouse/apiData/transformed/capacities_users_dimCapacityUsers'   ### ADD a version number to the folder path
-- MAGIC overwriteDeltaTable(toDf('dimCapacityUsers').drop('processedDate', 'processedTimestamp'), writePath)

-- COMMAND ----------

-- MAGIC %python
-- MAGIC sql_optimize_vacuum('transformed', 'capacities_users_dimCapacityUsers', writePath)

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ##Drop Views

-- COMMAND ----------

DROP VIEW capacities;
DROP VIEW users;
DROP VIEW dimCapacities;
DROP VIEW dimCapacityUsers;
