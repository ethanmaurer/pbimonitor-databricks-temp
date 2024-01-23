-- Databricks notebook source
-- MAGIC %md
-- MAGIC # Scan Result Flatten
-- MAGIC This notebook takes the table from 020_load and flattens the scan results into workspaces, datasources, and misconfigured datasources tables

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ##Functions and Variables

-- COMMAND ----------

-- MAGIC %run ./010_dependencies

-- COMMAND ----------

-- MAGIC %python
-- MAGIC
-- MAGIC topic = dbutils.widgets.text("topic", "", "")
-- MAGIC topic = dbutils.widgets.get("topic")
-- MAGIC
-- MAGIC database = 'transformed'
-- MAGIC readTable = f'{topic}'
-- MAGIC
-- MAGIC writePathRoot = f'/mnt/data-warehouse/apiData/{database}/{topic}'

-- COMMAND ----------

-- MAGIC %python
-- MAGIC
-- MAGIC spark.sql(f'''
-- MAGIC CREATE OR REPLACE TEMP VIEW temp
-- MAGIC AS (
-- MAGIC   SELECT *
-- MAGIC   FROM {database}.{readTable}
-- MAGIC )
-- MAGIC ''')

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ##Build Flattened Tables

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ###Datasources

-- COMMAND ----------

CREATE OR REPLACE TEMP VIEW datasources
AS (
  SELECT json.* EXCEPT (connectionDetails)
  , json.connectionDetails.*
  , processedTimeStamp
  , processedDate
  FROM temp
  LATERAL VIEW explode(datasourceInstances) AS json
  WHERE processedTimeStamp = (
    SELECT MAX(processedTimeStamp)
    FROM temp
  )
)

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ###Misconfigured datasources

-- COMMAND ----------

CREATE OR REPLACE TEMP VIEW misconfigured
AS (
  SELECT json.* EXCEPT (connectionDetails)
  , json.connectionDetails.*
  , processedTimeStamp
  , processedDate
  FROM temp
  LATERAL VIEW explode(misconfiguredDatasourceInstances) AS json
  WHERE processedTimeStamp = (
    SELECT MAX(processedTimeStamp)
    FROM temp
  )
)

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ###Workspaces

-- COMMAND ----------

CREATE OR REPLACE TEMP VIEW workspaces
AS (  
  SELECT json.id AS workspaceId
  , json.* EXCEPT (id, users)
  , json.users AS workspaceUsers
  , processedTimeStamp
  , processedDate
  FROM temp
  LATERAL VIEW explode(workspaces) AS json
  WHERE processedTimeStamp = (
    SELECT MAX(processedTimeStamp)
    FROM temp
  )
)

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ##Overwrite and Optimize

-- COMMAND ----------

-- MAGIC %md
-- MAGIC
-- MAGIC Add a parameter at the top of the notebook for '/mnt/data-warehouse/apiData/transformed'
-- MAGIC

-- COMMAND ----------

-- MAGIC %python
-- MAGIC
-- MAGIC datasourcesPath = f'{writePathRoot}_datasources/delta'  ### REMOVE the /delta don't need to store data in this folder.   ADD a version number to the folder path
-- MAGIC overwriteDeltaTable(toDf('datasources'), datasourcesPath)

-- COMMAND ----------

-- MAGIC %python
-- MAGIC
-- MAGIC misconfiguredPath = f'{writePathRoot}_misconfiguredDatasources/delta'   ### REMOVE the /delta don't need to store data in this folder.   ADD a version number to the folder path
-- MAGIC overwriteDeltaTable(toDf('misconfigured'), misconfiguredPath)

-- COMMAND ----------

-- MAGIC %python
-- MAGIC
-- MAGIC workspacesPath = f'{writePathRoot}_workspaces/delta'  ### REMOVE the /delta don't need to store data in this folder.   ADD a version number to the folder path
-- MAGIC overwriteDeltaTable(toDf('workspaces'), workspacesPath)

-- COMMAND ----------

-- MAGIC %python
-- MAGIC sql_optimize_vacuum(database, f'{topic}_datasources', datasourcesPath)

-- COMMAND ----------

-- MAGIC %python
-- MAGIC sql_optimize_vacuum(database, f'{topic}_misconfiguredDatasources', misconfiguredPath)

-- COMMAND ----------

-- MAGIC %python
-- MAGIC sql_optimize_vacuum(database, f'{topic}_workspaces', workspacesPath)

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ##Drop Views

-- COMMAND ----------

DROP VIEW datasources;
DROP VIEW misconfigured;
DROP VIEW workspaces;
