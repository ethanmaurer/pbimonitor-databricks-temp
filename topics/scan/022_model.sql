-- Databricks notebook source
-- MAGIC %md
-- MAGIC # Scan Model
-- MAGIC This notebook reads in the transformed.scan_result flattened tables and creates dim tables

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Functions and Variables

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
-- MAGIC writePathRoot = f'/mnt/data-warehouse/apiData/{database}/{topic}'  ### ADD a version number to the folder path
-- MAGIC
-- MAGIC tables = [
-- MAGIC     'workspaces',
-- MAGIC     'reports',
-- MAGIC     'dashboards',
-- MAGIC     'datasets',
-- MAGIC     'dataflows',
-- MAGIC     'datamarts'
-- MAGIC ]
-- MAGIC

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ##Workspaces

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Load workspaces

-- COMMAND ----------

-- MAGIC %python
-- MAGIC
-- MAGIC spark.sql(f'''
-- MAGIC CREATE OR REPLACE TEMP VIEW tempWorkspaces
-- MAGIC AS (
-- MAGIC   SELECT *
-- MAGIC   FROM {database}.{readTable}_workspaces 
-- MAGIC )
-- MAGIC ''')

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Create artifact dims

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #### Workspaces

-- COMMAND ----------

CREATE OR REPLACE TEMP VIEW workspaces
AS (
  SELECT workspaceId
  , name AS workspaceName
  , description
  , type
  , state
  , isOnDedicatedCapacity
  , workspaceUsers AS users
  FROM tempWorkspaces
)

-- COMMAND ----------

CREATE OR REPLACE TEMP VIEW dimWorkspaces
AS (
  SELECT * EXCEPT (users)
  FROM workspaces
)

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #### Reports

-- COMMAND ----------

CREATE OR REPLACE TEMP VIEW reports
AS (
  SELECT 
    workspaceId 
    ,json.id AS reportId
    ,json.* EXCEPT (id)
  FROM tempWorkspaces
  lateral view explode(reports) as json
)

-- COMMAND ----------

CREATE OR REPLACE TEMP VIEW dimReports
AS (
  SELECT * EXCEPT (users)
  FROM reports
)

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ####Dashboards

-- COMMAND ----------

CREATE OR REPLACE TEMP VIEW dashboards
AS (
  SELECT workspaceId
  , json.id AS dashboardId
  , json.displayName AS dashboardName
  , json.* EXCEPT (id, displayName)
  FROM tempWorkspaces
  LATERAL VIEW explode(dashboards) AS json
)

-- COMMAND ----------

CREATE OR REPLACE TEMP VIEW dimDashboards
AS (
  SELECT * EXCEPT (Tiles, users)
  FROM dashboards
)

-- COMMAND ----------

CREATE OR REPLACE TEMP VIEW dimTiles
AS (
  SELECT dashboardId
  , json.*
  FROM dashboards
  LATERAL VIEW explode(tiles) AS json
)

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #### Datasets

-- COMMAND ----------

CREATE OR REPLACE TEMP VIEW datasets
AS (
  SELECT workspaceId
  , json.id AS datasetId
  , json.name AS datasetName
  , json.* EXCEPT (name, id)
  FROM tempWorkspaces
  LATERAL VIEW explode(datasets) AS json
)

-- COMMAND ----------

CREATE OR REPLACE TEMP VIEW dimDatasets
AS (
  SELECT * EXCEPT (Expressions, Tables, users)
  FROM datasets
)

-- COMMAND ----------

CREATE OR REPLACE TEMP VIEW dimExpressions
AS (
  SELECT DatasetId
  , json.*
  FROM datasets
  LATERAL VIEW explode(Expressions) AS json
)

-- COMMAND ----------

CREATE OR REPLACE TEMP VIEW dimTables
AS (
  SELECT DatasetId
  , json.*
  FROM datasets
  LATERAL VIEW explode(Tables) AS json
)

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #### Dataflows

-- COMMAND ----------

CREATE OR REPLACE TEMP VIEW dataflows
AS (
  SELECT workspaceId
  , json.objectId AS dataflowId
  , json.name AS dataflowName
  , json.* EXCEPT (objectId, name)
  FROM tempWorkspaces
  LATERAL VIEW explode(dataflows) AS json
)

-- COMMAND ----------

CREATE OR REPLACE TEMP VIEW dimDataflows
AS (
  SELECT * EXCEPT (datasourceUsages, upstreamDataflows, users)
  FROM dataflows
)

-- COMMAND ----------

CREATE OR REPLACE TEMP VIEW dimDataflowSourceUsages
AS (
  SELECT dataflowId 
  , json.*
  FROM dataflows
  LATERAL VIEW explode(datasourceUsages) AS json
)

-- COMMAND ----------

CREATE OR REPLACE TEMP VIEW dimDataflowsUpstream
AS (
  SELECT dataflowId 
  , json.*
  FROM dataflows
  LATERAL VIEW explode(upstreamDataflows) AS json
)

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #### Datamarts

-- COMMAND ----------

CREATE OR REPLACE TEMP VIEW datamarts
AS (
  SELECT workspaceId
  , json.id AS datamartId
  , json.name AS datamartName
  , json.* EXCEPT (name, id)
  FROM tempWorkspaces
  LATERAL VIEW explode(datamarts) AS json
)

-- COMMAND ----------

CREATE OR REPLACE TEMP VIEW dimDatamarts
AS (
  SELECT * EXCEPT (datasourceUsages, users)
  FROM datamarts
)

-- COMMAND ----------

CREATE OR REPLACE TEMP VIEW dimDatamartSourceUsages
AS (
  SELECT datamartId
  , json.*
  FROM datamarts
  LATERAL VIEW explode(datasourceUsages) AS json
)

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Create Artifact Users Dims

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ####Artifact Users Loops

-- COMMAND ----------

-- MAGIC %python
-- MAGIC
-- MAGIC for table in tables:
-- MAGIC     spark.sql(f'''
-- MAGIC     CREATE OR REPLACE TEMP VIEW dim{table[:-1]}Users
-- MAGIC     AS (
-- MAGIC         SELECT {table[:-1]}Id
-- MAGIC         , json.*
-- MAGIC         FROM {table}
-- MAGIC         LATERAL VIEW explode(users) AS json
-- MAGIC     )
-- MAGIC     ''')

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Overwrite and Optimize

-- COMMAND ----------

-- MAGIC %md
-- MAGIC
-- MAGIC Might need to rename the dim[TableName]Users table to something else.  This table holds the relationship betwen all report and user level access to reports.
-- MAGIC
-- MAGIC > **Note:** Review with dan

-- COMMAND ----------

-- MAGIC %python
-- MAGIC workspacesDims = ['dimWorkspaces', 'dimWorkspaceUsers', 'dimReports', 'dimReportUsers', 'dimDashboards', 'dimDashboardUsers', 'dimDatasets', 'dimDatasetUsers', 'dimDataflows', 'dimDataflowUsers', 'dimDatamarts', 'dimDatamartUsers', 'dimDataflowSourceUsages', 'dimDataflowsUpstream', 'dimDatamartSourceUsages', 'dimExpressions', 'dimTables', 'dimTiles']

-- COMMAND ----------

-- MAGIC %python
-- MAGIC import time
-- MAGIC
-- MAGIC for table in workspacesDims:
-- MAGIC     start = time.time()
-- MAGIC     overwriteDeltaTable(toDf(table), f'{writePathRoot}_{table}/delta')   ### REMOVE the /delta don't need to store data in this folder.   ADD a version number to the folder path
-- MAGIC     end = time.time()
-- MAGIC     dif = round(end - start, 2)
-- MAGIC     print(f'{table} took {dif} seconds')

-- COMMAND ----------

-- MAGIC %python
-- MAGIC
-- MAGIC for table in workspacesDims:
-- MAGIC     start = time.time()
-- MAGIC     sql_optimize_vacuum(database, f'{topic}_{table}', f'{writePathRoot}_{table}/delta')
-- MAGIC     end = time.time()
-- MAGIC     dif = round(end - start, 2)
-- MAGIC     print(f'{table} took {dif} seconds')

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Drop Temp Views

-- COMMAND ----------

-- MAGIC %python
-- MAGIC for table in tables:
-- MAGIC     spark.sql(f'DROP VIEW {table}')

-- COMMAND ----------

-- MAGIC %python
-- MAGIC
-- MAGIC for table in workspacesDims:
-- MAGIC     spark.sql(f'DROP VIEW {table}')

-- COMMAND ----------

DROP VIEW tempWorkspaces

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ##Datasources

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ###Load Datasources As Dim

-- COMMAND ----------

-- MAGIC %python
-- MAGIC
-- MAGIC spark.sql(f'''
-- MAGIC CREATE OR REPLACE TEMP VIEW dimDatasources
-- MAGIC AS (
-- MAGIC   SELECT *
-- MAGIC   FROM {database}.{topic}_datasources
-- MAGIC )
-- MAGIC ''')

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Overwrite and Optimize

-- COMMAND ----------

-- MAGIC %python
-- MAGIC overwriteDeltaTable(toDf('dimDatasources'), f'{writePathRoot}_dimDatasources/delta')

-- COMMAND ----------

-- MAGIC %python
-- MAGIC sql_optimize_vacuum(database, f'{topic}_dimDatasources', f'{writePathRoot}_dimDatasources/delta')

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ###Drop Temp Views

-- COMMAND ----------

DROP VIEW dimDatasources

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ##Misconfigured Datasources

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ###Load Datasources As Dim

-- COMMAND ----------

-- MAGIC %python
-- MAGIC
-- MAGIC spark.sql(f'''
-- MAGIC CREATE OR REPLACE TEMP VIEW dimMisconfiguredDatasources
-- MAGIC AS (
-- MAGIC   SELECT *
-- MAGIC   FROM {database}.{topic}_misconfigureddatasources
-- MAGIC )
-- MAGIC ''')

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Overwrite and Optimize

-- COMMAND ----------

-- MAGIC %python
-- MAGIC overwriteDeltaTable(toDf('dimMisconfiguredDatasources'), f'{writePathRoot}_dimMisconfiguredDatasources/delta') ### REMOVE the /delta don't need to store data in this folder.   ADD a version number to the folder path

-- COMMAND ----------

-- MAGIC %python
-- MAGIC sql_optimize_vacuum(database, f'{topic}_dimMisconfiguredDatasources', f'{writePathRoot}_dimMisconfiguredDatasources/delta')  ### REMOVE the /delta don't need to store data in this folder.   ADD a version number to the folder path

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ###Drop Temp Views

-- COMMAND ----------

DROP VIEW dimMisconfiguredDatasources
