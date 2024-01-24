-- Databricks notebook source
-- MAGIC %md 
-- MAGIC ### PBI Pipelines Raw

-- COMMAND ----------

CREATE STREAMING TABLE pipelines_raw 
TBLPROPERTIES ("quality" = "bronze")
AS
SELECT 
pbipPipelines.*,
processedTimeStamp,
TenantId
    FROM STREAM read_files("/Volumes/pbimonitor/base/files/PBIPipelines/*",  multiLine=> true, schema => '
  STRUCT<
    value: ARRAY<
      STRUCT<
        id: STRING,
        displayName: STRING,
        description: STRING,
        stages: ARRAY<
          STRUCT<
            order: STRING,
            workspaceId: STRING,
            workspaceName: STRING
          >
        >
      >
    >,
    processedTimeStamp: STRING,
    processedDate: STRING,
    TenantId: STRING
  >
')
    LATERAL VIEW explode(value) AS pbipPipelines
  

-- COMMAND ----------

-- MAGIC %md
-- MAGIC Unique Keys = `id`

-- COMMAND ----------

CREATE STREAMING TABLE pipelines_current 

-- COMMAND ----------

APPLY CHANGES INTO LIVE.pipelines_current
FROM STREAM(LIVE.pipelines_raw)
KEYS(id)
SEQUENCE BY processedTimeStamp

-- COMMAND ----------

-- MAGIC %md 
-- MAGIC ### PBI Pipelines Users Raw

-- COMMAND ----------

CREATE STREAMING TABLE pipelinesUsers_raw 
TBLPROPERTIES ("quality" = "bronze")
AS
SELECT 
pipelineId,
pbipPipelinesUsers.*,
processedTimeStamp,
TenantId
    FROM STREAM read_files("/Volumes/pbimonitor/base/files/PBIPipelines_users/*",  multiLine=> true, schema => '
  STRUCT<
    value: ARRAY<
      STRUCT<
        accessRight: STRING,
        identifier: STRING,
        principalType: STRING
      >
    >,
    processedTimeStamp: STRING,
    processedDate: STRING,
    pipelineId: STRING,
    TenantId: STRING
  >
')
    LATERAL VIEW explode(value) AS pbipPipelinesUsers

-- COMMAND ----------

-- MAGIC %md
-- MAGIC # Important
-- MAGIC Unique Keys = ? (same issue as capacitiesUsers. Key seems to be two columns, not one, is this possible?)

-- COMMAND ----------

CREATE STREAMING TABLE pipelinesUsers_current 

-- COMMAND ----------

APPLY CHANGES INTO LIVE.pipelinesUsers_current
FROM STREAM(LIVE.pipelinesUsers_raw)
KEYS(pipelineId)
SEQUENCE BY processedTimeStamp

-- COMMAND ----------

CREATE OR REFRESH LIVE TABLE dimPipelines_pipelines
AS (
  SELECT * EXCEPT (id, stages, processedTimeStamp)
  , id AS pipelineId
  , stages[0].workspaceId AS stage1WorkspaceId
  , stages[1].workspaceId AS stage2WorkspaceId
  , stages[2].workspaceId AS stage3WorkspaceId
  FROM LIVE.pipelines_current
)

-- COMMAND ----------

CREATE OR REFRESH LIVE TABLE dimUsers_piplines
AS (
  SELECT * EXCEPT (processedTimeStamp)
  FROM LIVE.pipelinesUsers_current
)
