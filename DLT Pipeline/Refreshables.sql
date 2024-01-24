-- Databricks notebook source
CREATE STREAMING TABLE refreshables_raw 
TBLPROPERTIES ("quality" = "bronze")
AS
SELECT 
refreshables.*,
processedTimeStamp,
TenantId
    FROM STREAM read_files("/Volumes/pbimonitor/base/files/refreshables/*",  multiLine=> true, schema => '
  STRUCT<
    value: ARRAY<
      STRUCT<
        id: STRING,
        name: STRING,
        kind: STRING,
        lastRefresh: STRUCT<
          id: STRING,
          refreshType: STRING,
          startTime: STRING,
          endTime: STRING,
          serviceExceptionJson: STRING,
          status: STRING,
          requestId: STRING,
          extendedStatus: STRING
        >,
        refreshSchedule: STRUCT<
          days: ARRAY<STRING>,
          times: ARRAY<STRING>,
          enabled: STRING,
          localTimeZoneId: STRING,
          notifyOption: STRING
        >,
        configuredBy: ARRAY<STRING>,
        capacity: STRUCT<
          id: STRING,
          displayName: STRING,
          sku: STRING
        >,
        group: STRUCT<
          id: STRING,
          name: STRING
        >
      >
    >,
    processedDate: STRING,
    processedTimeStamp: STRING,
    TenantId: STRING
  >
'
)
LATERAL VIEW explode(value) AS refreshables


-- COMMAND ----------

-- MAGIC %md
-- MAGIC Unique Key = `id`

-- COMMAND ----------

CREATE STREAMING TABLE refreshables_current 

-- COMMAND ----------

APPLY CHANGES INTO LIVE.refreshables_current
FROM STREAM(LIVE.refreshables_raw)
KEYS(id)
SEQUENCE BY processedTimeStamp

-- COMMAND ----------

-- MAGIC %md 
-- MAGIC ### Dim Refreshables

-- COMMAND ----------

CREATE OR REFRESH LIVE TABLE dimRefreshables_refreshables
AS 
SELECT * EXCEPT (capacity, group, lastRefresh, refreshSchedule, configuredBy, processedTimeStamp)
  , capacity.id AS capacityId
  , group.id AS workspaceId
  , refreshSchedule.enabled AS scheduleEnabled
  , refreshSchedule.notifyOption AS notifyOption
FROM LIVE.refreshables_current


-- COMMAND ----------

-- MAGIC %md
-- MAGIC ###Fact lastRefresh

-- COMMAND ----------

CREATE OR REFRESH LIVE TABLE factLastRefresh_refreshables
AS
SELECT id AS refreshableId
  , lastRefresh.id AS refreshId
  , lastRefresh.* EXCEPT (id)
  , TenantId
FROM LIVE.refreshables_current
WHERE lastRefresh IS NOT NULL


-- COMMAND ----------

-- MAGIC %md
-- MAGIC ###Fact refreshSchedule

-- COMMAND ----------

CREATE OR REFRESH LIVE TABLE factRefreshSchedule_refreshables
AS
SELECT id AS refreshableId
  , refreshSchedule.* EXCEPT (days, times, localTimeZoneId, enabled, notifyOption)
  , ARRAY_CONTAINS(refreshSchedule.days, "Sunday") AS Sunday 
  , ARRAY_CONTAINS(refreshSchedule.days, "Monday") AS Monday
  , ARRAY_CONTAINS(refreshSchedule.days, "Tuesday") AS Tuesday
  , ARRAY_CONTAINS(refreshSchedule.days, "Wednesday") AS Wednesday
  , ARRAY_CONTAINS(refreshSchedule.days, "Thursday") AS Thursday
  , ARRAY_CONTAINS(refreshSchedule.days, "Friday") AS Friday
  , ARRAY_CONTAINS(refreshSchedule.days, "Saturday") AS Saturday
  , TenantId 
FROM LIVE.refreshables_current


-- COMMAND ----------

-- MAGIC %md
-- MAGIC ###Fact refreshTimes

-- COMMAND ----------

CREATE OR REFRESH LIVE TABLE factRefreshTimes_refreshables
AS
SELECT id AS refreshableId
  , time
  , refreshSchedule.localTimeZoneId
  , TenantId
FROM LIVE.refreshables_current
LATERAL VIEW explode(refreshSchedule.times) AS time

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ###Fact configuredBy

-- COMMAND ----------

CREATE OR REFRESH LIVE TABLE factConfiguredBy_refreshables
AS
SELECT id AS refreshableId
  , configurer
  , TenantId
FROM LIVE.refreshables_current
LATERAL VIEW explode(configuredBy) AS configurer

