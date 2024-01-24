-- Databricks notebook source
CREATE STREAMING TABLE tenantSettings_raw
TBLPROPERTIES ("quality" = "bronze")
AS
SELECT 
  settings.*,
  processedTimeStamp,
  TenantId
FROM STREAM read_files("/Volumes/pbimonitor/base/files/tenantSettings/*", multiLine => true, schema => '
  STRUCT<
    tenantSettings: ARRAY<
      STRUCT<
        settingName: STRING,
        title: STRING,
        enabled: STRING,
        canSpecifySecurityGroups: STRING,
        enabledSecurityGroups: ARRAY<
          STRUCT<
            graphId: STRING,
            name: STRING
          >
        >,
        tenantSettingGroup: STRING
      >
    >,
    processedTimeStamp: STRING,
    processedDate: STRING,
    TenantId: STRING
  >
')
LATERAL VIEW explode(tenantSettings) AS settings


-- COMMAND ----------

-- MAGIC %md
-- MAGIC Unique Keys = `settingName` **or** `title` 

-- COMMAND ----------

CREATE STREAMING TABLE tenantSettings_current

-- COMMAND ----------

APPLY CHANGES INTO LIVE.tenantSettings_current
FROM STREAM(LIVE.tenantSettings_raw)
KEYS(title)
SEQUENCE BY processedTimeStamp

-- COMMAND ----------

-- MAGIC %md 
-- MAGIC ### Dim Tenant Settings

-- COMMAND ----------

CREATE OR REFRESH LIVE TABLE dimTenantSettings_tenant
AS (
  SELECT * EXCEPT (enabledSecurityGroups)
  FROM LIVE.tenantSettings_current
)

-- COMMAND ----------

-- MAGIC %md 
-- MAGIC ### Dim Enambed Security Groups
-- MAGIC

-- COMMAND ----------

CREATE OR REFRESH LIVE TABLE dimEnabledSecurityGroups_tenant
AS (
  SELECT settingName
  , groups.graphId AS groupId
  , TenantId
  FROM LIVE.tenantSettings_current
  LATERAL VIEW explode(enabledSecurityGroups) AS groups
)
