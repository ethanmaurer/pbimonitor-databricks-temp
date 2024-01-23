-- Databricks notebook source
-- MAGIC %md
-- MAGIC # MSGraph Model
-- MAGIC This notebook reads the tables from the 020_load notebook and creates dimension tables 

-- COMMAND ----------

-- MAGIC %run ./010_dependencies

-- COMMAND ----------

-- MAGIC %python
-- MAGIC tables = ["securityGroups", "subscribedSKUs", "users", "usersInSecurityGroups"]
-- MAGIC
-- MAGIC for table in tables:
-- MAGIC     spark.sql(f'''
-- MAGIC               CREATE OR REPLACE TEMP VIEW {table}
-- MAGIC               AS (
-- MAGIC                   SELECT *
-- MAGIC                   FROM transformed.msgraph_{table}
-- MAGIC                   WHERE processedTimeStamp = (
-- MAGIC                       SELECT MAX (processedTimeStamp)
-- MAGIC                       FROM transformed.msgraph_{table}
-- MAGIC                   )
-- MAGIC               )
-- MAGIC               ''')

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Dimensions

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Users

-- COMMAND ----------

CREATE OR REPLACE TEMP VIEW dimUsers
AS (
  SELECT * EXCEPT (assignedLicenses, memberOf)
  FROM users
)

-- COMMAND ----------

CREATE OR REPLACE TEMP VIEW dimAssignedLicenses
AS (
  SELECT UserId, json.*
  FROM users
  LATERAL VIEW explode(assignedLicenses) AS json
)

-- COMMAND ----------

CREATE OR REPLACE TEMP VIEW dimMemberOf
AS (
  SELECT UserId, json.*
  FROM users
  LATERAL VIEW explode(memberOf) AS json
)

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Security Groups

-- COMMAND ----------

CREATE OR REPLACE TEMP VIEW dimSecurityGroups
AS (
  SELECT *
  FROM securityGroups
)

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Subscribed SKUs
-- MAGIC I don't understand this dim  
-- MAGIC Mike: I think this comes from a user that has a subscription attached to a report within Power BI. Would need to confirm this. 

-- COMMAND ----------

CREATE OR REPLACE TEMP VIEW dimSubscribedSKUs
AS (
SELECT * EXCEPT (prepaidUnits)
, prepaidUnits.*
FROM subscribedSKUs
)

-- COMMAND ----------

SELECT *
FROM dimSubscribedSKUs

-- COMMAND ----------

CREATE OR REPLACE TEMP VIEW dimSubscriptionIds
AS (
  SELECT subscribedSKUId, exploded AS subscriptionIds
  FROM subscribedSKUs
  LATERAL VIEW explode(subscriptionIds) AS exploded
)

-- COMMAND ----------

SELECT *
FROM dimSubscriptionIds

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Users In Security Groups

-- COMMAND ----------

CREATE OR REPLACE TEMP VIEW dimUsersInSecurityGroups
AS (
  SELECT *
  FROM usersInSecurityGroups
)

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Overwrite and Optimize Tables

-- COMMAND ----------

-- MAGIC %python
-- MAGIC import time
-- MAGIC
-- MAGIC dims = ['dimUsers', 'dimAssignedLicenses', 'dimMemberOf', 'dimSecurityGroups', 'dimSubscribedSKUs', 'dimUsersInSecurityGroups']
-- MAGIC
-- MAGIC for table in dims:
-- MAGIC     writePath = f'/mnt/data-warehouse/apiData/transformed/msgraph_{table}/delta'
-- MAGIC     start = time.time()
-- MAGIC     overwriteDeltaTable(toDf(table), writePath)
-- MAGIC     end = time.time()
-- MAGIC     dif = round(end - start, 2)
-- MAGIC     print(f'{table} took {dif} seconds')

-- COMMAND ----------

-- MAGIC %python
-- MAGIC for table in dims:
-- MAGIC     writePath = f'/mnt/data-warehouse/apiData/transformed/msgraph_{table}/delta'
-- MAGIC     start = time.time()
-- MAGIC     sql_optimize_vacuum('transformed', f'msgraph_{table}', writePath)
-- MAGIC     end = time.time()
-- MAGIC     dif = round(end - start, 2)
-- MAGIC     print(f'{table} took {dif} seconds')

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Drop Views

-- COMMAND ----------

-- MAGIC %python
-- MAGIC for table in dims:
-- MAGIC     writePath = f'/mnt/data-warehouse/apiData/transformed/msgraph_{table}/delta'
-- MAGIC     start = time.time()
-- MAGIC     spark.sql(f'DROP VIEW {table}')
-- MAGIC     end = time.time()
-- MAGIC     dif = round(end - start, 2)
-- MAGIC     print(f'{table} took {dif} seconds')
