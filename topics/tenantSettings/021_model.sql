-- Databricks notebook source
-- MAGIC %md
-- MAGIC # Tenant Settings Model
-- MAGIC This notebook reads the table from the 020_load notebook and creates dimension tables 

-- COMMAND ----------

-- MAGIC %run ./010_dependencies

-- COMMAND ----------

CREATE OR REPLACE TEMP VIEW settings
AS (
  SELECT *
  FROM transformed.tenantSettings
)

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Dimensions

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Tenant Settings

-- COMMAND ----------

CREATE OR REPLACE TEMP VIEW dimTenantSettings
AS (
  SELECT * EXCEPT (enabledSecurityGroups)
  FROM settings
)

-- COMMAND ----------

CREATE OR REPLACE TEMP VIEW dimEnabledSecurityGroups
AS (
  SELECT settingName
  , groups.*
  FROM settings
  LATERAL VIEW explode(enabledSecurityGroups) AS groups
)

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Overwrite and Optimize Tables

-- COMMAND ----------

-- MAGIC %python
-- MAGIC writePath = '/mnt/data-warehouse/apiData/transformed/tenantSettings_dimTenantSettings'
-- MAGIC overwriteDeltaTable(toDf('dimTenantSettings').drop('processedDate', 'processedTimestamp'), writePath)

-- COMMAND ----------

-- MAGIC %python
-- MAGIC sql_optimize_vacuum('transformed', 'tenantSettings_dimTenantSettings', writePath)

-- COMMAND ----------

-- MAGIC %python
-- MAGIC writePath = '/mnt/data-warehouse/apiData/transformed/tenantSettings_dimEnabledSecurityGroups'
-- MAGIC overwriteDeltaTable(toDf('dimEnabledSecurityGroups').drop('processedDate', 'processedTimestamp'), writePath)

-- COMMAND ----------

-- MAGIC %python
-- MAGIC sql_optimize_vacuum('transformed', 'tenantSettings_dimEnabledSecurityGroups', writePath)

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Drop Views

-- COMMAND ----------

DROP VIEW settings;
DROP VIEW dimTenantSettings;
DROP VIEW dimEnabledSecurityGroups;
