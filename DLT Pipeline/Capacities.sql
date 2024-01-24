-- Databricks notebook source
-- MAGIC %md 
-- MAGIC ### Capacities Raw
-- MAGIC

-- COMMAND ----------

CREATE STREAMING TABLE capacities_raw 
TBLPROPERTIES("quality" = "bronze")
AS

SELECT 
capacities.*,
TenantId,
processedTimeStamp
  
  FROM STREAM read_files("/Volumes/pbimonitor/base/files/capacities/*",  multiLine=> true, schema=>'
    STRUCT<
      value: ARRAY<
        STRUCT<
          id: STRING,
          displayName: STRING,
          admins: ARRAY<STRING>,
          sku: STRING,
          state: STRING,
          capacityUserAccessRight: STRING,
          region: STRING,
          users: ARRAY<STRING>
        >
      >,
      processedTimeStamp: STRING,
      processedDate: STRING,
      TenantId: STRING
  >
  ')
  LATERAL VIEW explode(value) AS capacities
  

-- COMMAND ----------

-- MAGIC %md
-- MAGIC Unique Keys = `id` 

-- COMMAND ----------

CREATE STREAMING TABLE capacities_current 
TBLPROPERTIES("quality" = "silver")

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Apply Changes
-- MAGIC

-- COMMAND ----------

APPLY CHANGES INTO LIVE.capacities_current
FROM STREAM(LIVE.capacities_raw)
KEYS(id)
SEQUENCE BY processedTimeStamp

-- COMMAND ----------

-- MAGIC %md 
-- MAGIC ### Capacities Users Raw

-- COMMAND ----------

CREATE STREAMING TABLE capacitiesUsers_raw AS

SELECT 
capacitiesUsers.*,
TenantId,
processedTimeStamp
    FROM STREAM read_files("/Volumes/pbimonitor/base/files/capacities_users/*",  multiLine=> true, schema=>'
    
        STRUCT<
            capacityUsers:ARRAY<
                STRUCT<
                    capacityUserAccessRight:STRING,
                    emailAddress:STRING,
                    displayName:STRING,
                    identifier:STRING,
                    graphId:STRING,
                    principalType:STRING,
                    profile:STRING
                >
            >,
            processedDate:STRING,
            processedTimeStamp:STRING,
            capacityId:STRING,
            `@odata.context`:STRING,
            TenantId: STRING
        >
    
    ')
    LATERAL VIEW explode(capacityUsers) AS capacitiesUsers
  

-- COMMAND ----------

-- MAGIC %md 
-- MAGIC # Important
-- MAGIC Unique Key = ? (It may be identifier but the table has no data for some reason. Is it possible for the unique key to be two different columns? If so Key = capacitiyId and userId)

-- COMMAND ----------

CREATE STREAMING TABLE capacitiesUsers_current

-- COMMAND ----------

APPLY CHANGES INTO LIVE.capacitiesUsers_current
FROM STREAM(LIVE.capacitiesUsers_raw)
KEYS(identifier)
SEQUENCE BY processedTimeStamp

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Capacities

-- COMMAND ----------

CREATE OR REFRESH LIVE TABLE dimCapacities_capacities
AS (
  SELECT * EXCEPT(admins, capacityUserAccessRight, users, processedTimeStamp)
  FROM LIVE.capacities_current
)

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ###Capacity Users

-- COMMAND ----------

CREATE OR REFRESH LIVE TABLE dimUsers_capacities
AS (
  SELECT * EXCEPT(processedTimeStamp)
  FROM LIVE.capacitiesUsers_current
)
