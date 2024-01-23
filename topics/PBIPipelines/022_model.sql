-- Databricks notebook source
-- MAGIC %md
-- MAGIC # PBIPipeline Model
-- MAGIC This notebook reads the table from the 020_current and 021_users notebooks and creates dimension tables 

-- COMMAND ----------

-- MAGIC %run ./010_dependencies

-- COMMAND ----------

CREATE OR REPLACE TEMP VIEW pipelines
AS (
  SELECT *
  FROM transformed.pbipipelines
)

-- COMMAND ----------

CREATE OR REPLACE TEMP VIEW users
AS (
  SELECT *
  FROM transformed.pbipipelines_users
)

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Dimensions

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Pipelines

-- COMMAND ----------

CREATE OR REPLACE TEMP VIEW dimPipelines
AS (
  SELECT * EXCEPT (id, stages)
  , id AS pipelineId
  , stages[0].workspaceId AS stage1WorkspaceId
  , stages[1].workspaceId AS stage2WorkspaceId
  , stages[2].workspaceId AS stage3WorkspaceId
  FROM pipelines
)

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ###Pipeline Users

-- COMMAND ----------

CREATE OR REPLACE TEMP VIEW dimPipelineUsers
AS (
  SELECT *
  FROM users
)

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Overwrite and Optimize Tables

-- COMMAND ----------

-- MAGIC %python
-- MAGIC writePath = '/mnt/data-warehouse/apiData/transformed/pipelines_dimPipelines' ### ADD a version number to the folder path
-- MAGIC overwriteDeltaTable(toDf('dimPipelines').drop('processedDate', 'processedTimestamp'), writePath)

-- COMMAND ----------

-- MAGIC %python
-- MAGIC sql_optimize_vacuum('transformed', 'pbipipelines_dimPipelines', writePath)

-- COMMAND ----------

-- MAGIC %python
-- MAGIC writePath = '/mnt/data-warehouse/apiData/transformed/pbipipelines_users_dimPipelineUsers'
-- MAGIC overwriteDeltaTable(toDf('dimPipelineUsers').drop('processedDate', 'processedTimestamp'), writePath)

-- COMMAND ----------

-- MAGIC %python
-- MAGIC sql_optimize_vacuum('transformed', 'pbipipelines_users_dimPipelineUsers', writePath)

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ##Drop Views

-- COMMAND ----------

DROP VIEW pipelines;
DROP VIEW users;
DROP VIEW dimPipelines;
DROP VIEW dimPipelineUsers;
