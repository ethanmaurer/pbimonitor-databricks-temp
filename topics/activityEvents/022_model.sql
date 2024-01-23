-- Databricks notebook source
-- MAGIC %md
-- MAGIC # Activity Events Model
-- MAGIC This notebook reads the table from the 020_load notebook and creates fact and dimension tables 

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Functions and Variables Set-up

-- COMMAND ----------

-- MAGIC %run ./010_dependencies

-- COMMAND ----------

-- MAGIC %python
-- MAGIC topic = dbutils.widgets.text("topic", "", "")
-- MAGIC topic = dbutils.widgets.get("topic")
-- MAGIC
-- MAGIC database = 'transformed'
-- MAGIC readTable = f'{topic}'
-- MAGIC
-- MAGIC writePathRoot = f'/mnt/data-warehouse/apiData/{database}/{topic}' ### ADD a version number to the folder path

-- COMMAND ----------

-- MAGIC %python
-- MAGIC spark.sql(f'''
-- MAGIC CREATE OR REPLACE TEMP VIEW activityEventsTemp
-- MAGIC AS(
-- MAGIC SELECT *
-- MAGIC FROM {database}.{readTable}
-- MAGIC )
-- MAGIC ''')

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Expand Object Columns

-- COMMAND ----------

CREATE OR REPLACE TEMP VIEW activityEventsWObjects
AS (
  SELECT *
  , CASE WHEN ObjectType = 'DataFlow' THEN ObjectId ELSE NULL END AS DataFlowGen2Id
  , CASE WHEN ObjectType = 'DataFlow' THEN ObjectDisplayName ELSE NULL END AS DataFlowGen2Name
  , CASE WHEN ObjectType = 'Lakewarehouse' THEN ObjectId ELSE NULL END AS DefaultWarehouseId
  , CASE WHEN ObjectType = 'Lakewarehouse' THEN ObjectDisplayName ELSE NULL END AS DefaultWarehouseName
  , CASE WHEN ObjectType = 'Model' THEN ObjectId ELSE NULL END AS ModelId
  , CASE WHEN ObjectType = 'Model' THEN ObjectDisplayName ELSE NULL END AS ModelName
  , CASE WHEN ObjectType = 'Lakehouse' THEN ObjectId ELSE NULL END AS LakehouseId
  , CASE WHEN ObjectType = 'Lakehouse' THEN ObjectDisplayName ELSE NULL END AS LakehouseName
  , CASE WHEN ObjectType = 'Datawarehouse' THEN ObjectId ELSE NULL END AS DatawarehouseId
  , CASE WHEN ObjectType = 'Datawarehouse' THEN ObjectDisplayName ELSE NULL END AS DatawarehouseName
  , CASE WHEN ObjectType = 'EventStream' THEN ObjectId ELSE NULL END AS EventStreamId
  , CASE WHEN ObjectType = 'EventStream' THEN ObjectDisplayName ELSE NULL END AS EventStreamName
  , CASE WHEN ObjectType = 'LakeWarehouse' THEN ObjectId ELSE NULL END AS LakeWarehouseId
  , CASE WHEN ObjectType = 'LakeWarehouse' THEN ObjectDisplayName ELSE NULL END AS LakeWarehouseName 
  FROM activityEventsTemp
  )

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Dimensions

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Users

-- COMMAND ----------

CREATE OR REPLACE TEMP VIEW dimUsers
AS (
  WITH grouped AS (
    SELECT UserId, MAX(CreationTime) AS CreationTime
    FROM activityEventsWObjects
    WHERE UserId IS NOT NULL
    GROUP BY UserId
  )

  SELECT DISTINCT UserId, CreationTime
  FROM grouped
  LEFT JOIN activityEventsWObjects USING (UserId, CreationTime)
)

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Workspaces

-- COMMAND ----------

-- MAGIC %md
-- MAGIC
-- MAGIC There are two columns that mean the same thing. WorkspaceId column is the same as FolderObjectId 
-- MAGIC This query will merge both columns for Id and Name down to one complete list of Workspaces
-- MAGIC
-- MAGIC ```
-- MAGIC Folder = Workspace
-- MAGIC ```

-- COMMAND ----------

CREATE OR REPLACE TEMP VIEW workspacesDupes
AS (
    SELECT WorkspaceId
    , WorkspaceName AS Name
    , CreationTime
    FROM activityEventsWObjects

    UNION 
    
    SELECT FolderObjectId AS WorkspaceId
    , FolderDisplayName AS Name
    , CreationTime
    FROM activityEventsWObjects
)

-- COMMAND ----------

-- MAGIC %md
-- MAGIC > **Note:** When data is added with the braces `{}` indicate that data was created from the ETL process.

-- COMMAND ----------

CREATE OR REPLACE TEMP VIEW dimWorkspaces
AS (
  WITH grouped AS (
    SELECT WorkspaceId, MAX(CreationTime) AS CreationTime
    FROM workspacesDupes
    WHERE WorkspaceId IS NOT NULL AND Name IS NOT NULL
    GROUP BY WorkspaceId
  )

  SELECT WorkspaceId,
  CASE 
    WHEN WorkspaceId = '00000000-0000-0000-0000-000000000000' THEN '{0}' 
    ELSE Name
  END AS WorkspaceName
  , CreationTime
  FROM grouped
  LEFT JOIN (
    SELECT *
    FROM workspacesDupes 
    WHERE Name IS NOT NULL
  )
  USING (WorkspaceId, CreationTime)
)

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Datasets

-- COMMAND ----------

CREATE OR REPLACE TEMP VIEW dimDatasets
AS (
  WITH grouped AS (
    SELECT DatasetId, MAX(CreationTime) AS CreationTime
    FROM activityEventsWObjects
    WHERE DatasetId IS NOT NULL AND DatasetName IS NOT NULL
    GROUP BY DatasetId
  )

  SELECT DISTINCT DatasetId, DatasetName, CreationTime
  FROM grouped
  LEFT JOIN activityEventsWObjects USING (DatasetId, CreationTime)
)

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Reports

-- COMMAND ----------

CREATE OR REPLACE TEMP VIEW reportsDupes
AS (
    SELECT ReportId
    , ReportName AS ReportName
    , ReportType
    , CreationTime
    FROM activityEventsWObjects
    
    UNION 
    
    SELECT CopiedReportId AS ReportId
    , CopiedReportName AS ReportName
    , ReportType
    , CreationTime
    FROM activityEventsWObjects
)

-- COMMAND ----------

CREATE OR REPLACE TEMP VIEW dimReports
AS (
  WITH grouped AS (
    SELECT ReportId, MAX(CreationTime) AS CreationTime
    FROM reportsDupes
    WHERE ReportId IS NOT NULL AND ReportName IS NOT NULL
    GROUP BY ReportId
  )

  SELECT DISTINCT ReportId, ReportName, ReportType, CreationTime
  FROM grouped
  LEFT JOIN activityEventsWObjects USING (ReportId, CreationTime)
)

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Dashboards

-- COMMAND ----------

CREATE OR REPLACE TEMP VIEW dimDashboards
AS (
  WITH grouped AS (
    SELECT DashboardId, MAX(CreationTime) AS CreationTime
    FROM activityEventsWObjects
    WHERE DashboardId IS NOT NULL AND DashboardName IS NOT NULL
    GROUP BY DashboardId
  )

  SELECT DISTINCT DashboardId, DashboardName, CreationTime
  FROM grouped
  LEFT JOIN activityEventsWObjects USING (DashboardId, CreationTime)
)

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Apps

-- COMMAND ----------

CREATE OR REPLACE TEMP VIEW dimApps
AS (
  WITH grouped AS (
    SELECT AppId, MAX(CreationTime) AS CreationTime
    FROM activityEventsWObjects
    WHERE AppId IS NOT NULL
    GROUP BY AppId
  )

  SELECT DISTINCT AppId, AppName, AppReportId, CreationTime
  FROM grouped
  LEFT JOIN activityEventsWObjects USING (AppId, CreationTime)
)

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Dataflows 

-- COMMAND ----------

CREATE OR REPLACE TEMP VIEW dimDataflows
AS (
  WITH grouped AS (
    SELECT DataflowId, MAX(CreationTime) AS CreationTime
    FROM activityEventsWObjects
    WHERE DataflowId IS NOT NULL
    GROUP BY DataflowId
  )

  SELECT DISTINCT DataflowId, DataflowName, DataflowType, DataflowRefreshScheduleType, CreationTime
  FROM grouped
  LEFT JOIN activityEventsWObjects USING (DataflowId, CreationTime)
)

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Datasources

-- COMMAND ----------

CREATE OR REPLACE TEMP VIEW dimDatasources
AS (
  WITH grouped AS (
    SELECT DatasourceId, MAX(CreationTime) AS CreationTime
    FROM activityEventsWObjects
    WHERE DatasourceId IS NOT NULL
    GROUP BY DatasourceId
  )

  SELECT DISTINCT DatasourceId, DatasourceDetails, CreationTime
  FROM grouped
  LEFT JOIN activityEventsWObjects USING (DatasourceId, CreationTime)
)

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Capacities

-- COMMAND ----------

CREATE OR REPLACE TEMP VIEW dimCapacities
AS (
  WITH grouped AS (
    SELECT CapacityId, MAX(CreationTime) AS CreationTime
    FROM activityEventsWObjects
    WHERE CapacityId IS NOT NULL AND CapacityName IS NOT NULL
    GROUP BY CapacityId
  )

  SELECT DISTINCT CapacityId, CapacityName, CapacityState, CreationTime
  FROM grouped
  LEFT JOIN activityEventsWObjects USING (CapacityId, CreationTime)
)

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Pipelines

-- COMMAND ----------

CREATE OR REPLACE TEMP VIEW dimPipelines
AS (
  WITH grouped AS (
    SELECT DeploymentPipelineId, MAX(CreationTime) AS CreationTime
    FROM activityEventsWObjects
    WHERE DeploymentPipelineId IS NOT NULL
    GROUP BY DeploymentPipelineId
  )

  SELECT DISTINCT DeploymentPipelineId, DeploymentPipelineDisplayName, CreationTime
  FROM grouped
  LEFT JOIN activityEventsWObjects USING (DeploymentPipelineId, CreationTime)
)

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Imports

-- COMMAND ----------

CREATE OR REPLACE TEMP VIEW dimImports
AS (
  WITH grouped AS (
    SELECT importId, MAX(CreationTime) AS CreationTime
    FROM activityEventsWObjects
    WHERE ImportId IS NOT NULL
    GROUP BY importId
  )

  SELECT DISTINCT importId, importDisplayName, importType, importSource, CreationTime
  FROM grouped
  LEFT JOIN activityEventsWObjects USING (importId, CreationTime)
)

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### DataflowGen2s

-- COMMAND ----------

CREATE OR REPLACE TEMP VIEW dimDataflowGen2s
AS (
  WITH grouped AS (
    SELECT DataflowGen2Id, MAX(CreationTime) AS CreationTime
    FROM activityEventsWObjects
    WHERE DataflowGen2Id IS NOT NULL
    GROUP BY DataflowGen2Id
  )

  SELECT DISTINCT DataflowGen2Id, DataflowGen2Name, CreationTime
  FROM grouped
  LEFT JOIN activityEventsWObjects USING (DataflowGen2Id, CreationTime)
)

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### DefaultWarehouses

-- COMMAND ----------

CREATE OR REPLACE TEMP VIEW dimDefaultWarehouses
AS (
  WITH grouped AS (
    SELECT DefaultWarehouseId, MAX(CreationTime) AS CreationTime
    FROM activityEventsWObjects
    WHERE DefaultWarehouseId IS NOT NULL
    GROUP BY DefaultWarehouseId
  )

  SELECT DISTINCT DefaultWarehouseId, DefaultWarehouseName, CreationTime
  FROM grouped
  LEFT JOIN activityEventsWObjects USING (DefaultWarehouseId, CreationTime)
)

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Models

-- COMMAND ----------

CREATE OR REPLACE TEMP VIEW dimModels
AS (
  WITH grouped AS (
    SELECT modelId, MAX(CreationTime) AS CreationTime
    FROM activityEventsWObjects
    WHERE modelId IS NOT NULL
    GROUP BY modelId
  )

  SELECT DISTINCT modelId, modelName, CreationTime
  FROM grouped
  LEFT JOIN activityEventsWObjects USING (modelId, CreationTime)
)

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Lakehouses

-- COMMAND ----------

CREATE OR REPLACE TEMP VIEW dimLakehouses
AS (
  WITH grouped AS (
    SELECT lakehouseId, MAX(CreationTime) AS CreationTime
    FROM activityEventsWObjects
    WHERE lakehouseId IS NOT NULL
    GROUP BY lakehouseId
  )

  SELECT DISTINCT lakehouseId, lakehouseName, CreationTime
  FROM grouped
  LEFT JOIN activityEventsWObjects USING (lakehouseId, CreationTime)
)

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Datawarehouses

-- COMMAND ----------

CREATE OR REPLACE TEMP VIEW dimDatawarehouses
AS (
  WITH grouped AS (
    SELECT datawarehouseId, MAX(CreationTime) AS CreationTime
    FROM activityEventsWObjects
    WHERE datawarehouseId IS NOT NULL
    GROUP BY datawarehouseId
  )

  SELECT DISTINCT datawarehouseId, datawarehouseName, CreationTime
  FROM grouped
  LEFT JOIN activityEventsWObjects USING (datawarehouseId, CreationTime)
)

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### EventStreams

-- COMMAND ----------

CREATE OR REPLACE TEMP VIEW dimEventStreams
AS (
  WITH grouped AS (
    SELECT eventStreamId, MAX(CreationTime) AS CreationTime
    FROM activityEventsWObjects
    WHERE eventStreamId IS NOT NULL
    GROUP BY eventStreamId
  )

  SELECT DISTINCT eventStreamId, eventStreamName, CreationTime
  FROM grouped
  LEFT JOIN activityEventsWObjects USING (eventStreamId, CreationTime)
)

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### LakeWarehouses

-- COMMAND ----------

CREATE OR REPLACE TEMP VIEW dimLakeWarehouses
AS (
  WITH grouped AS (
    SELECT LakeWarehouseId, MAX(CreationTime) AS CreationTime
    FROM activityEventsWObjects
    WHERE LakeWarehouseId IS NOT NULL
    GROUP BY LakeWarehouseId
  )

  SELECT DISTINCT LakeWarehouseId, LakeWarehouseName, CreationTime
  FROM grouped
  LEFT JOIN activityEventsWObjects USING (LakeWarehouseId, CreationTime)
)

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Dates

-- COMMAND ----------

CREATE OR REPLACE TEMP VIEW dimDates
AS (
  WITH date_range AS (
    SELECT
      DATE_TRUNC('year', MIN(CreationTime)) AS start_date,
      DATE_TRUNC('year', MAX(CreationTime)) + INTERVAL '2 year' - INTERVAL '1 day' AS end_date
    FROM activityEventsWObjects
  )

  -- Generate a sequence of numbers from 0 to the number of days between the start and end date
  , date_sequence AS (
    SELECT explode(sequence(0, datediff(end_date, start_date))) AS seq
    FROM date_range
  )

-- Add the date column by adding the number of days to the start_date
  SELECT CAST(date_add(start_date, seq) AS DATE) AS date
  , DAY(date) AS day
  , MONTH(date) AS month
  , date_format(date, 'MMM') AS monthName 
  , YEAR(date) AS year
  , WEEKDAY(date) AS weekday
  , date_format(date, 'EEEE') AS weekdayName

  -- Create a grouping column for filtering reports that enable Future, Frequent, and Active
  , CASE 
    WHEN date > CURRENT_DATE THEN "Future"
    WHEN date >= CURRENT_DATE - INTERVAL '7 day' AND date <= CURRENT_DATE THEN "Frequent"
    WHEN date < CURRENT_DATE - INTERVAL '7 day' AND date >= CURRENT_DATE - INTERVAL "30 day" THEN "Active" 
    WHEN date < CURRENT_DATE - INTERVAL '30 day' THEN "Inactive"
  END AS Frequency
  FROM date_range, date_sequence
)

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Facts

-- COMMAND ----------

CREATE OR REPLACE TEMP VIEW fact
AS (
  SELECT * EXCEPT (RecordType, Activity, OrganizationId, CLientIP, Workload, ArtifactId, ArtifactKind, ArtifactName, ItemName, ObjectId, ObjectType, ObjectDisplayName, WorkSpaceName, DatasetName, ReportName, ReportType, DashboardName, AppReportId, AppName, DataflowName, DataflowType, DataflowRefreshScheduleType, DatasourceDetails, CapacityName, CapacityState, DeploymentPipelineAccesses, DeploymentPipelineDisplayName, ImportDisplayName, ImportType, ImportSource, DataFlowGen2Name, DefaultWarehouseName, ModelName, LakehouseName, DatawarehouseName, EventStreamName, LakeWarehouseName, activityDate, processedTimeStamp, processedDate, missingCols, CreationTime, FolderObjectId, FolderDisplayName, ResultStatus, IsSuccess, CopiedReportId, CopiedReportName)
  , COALESCE(ResultStatus, IsSuccess) AS Result
  , SPLIT(CreationTime, 'T')[0] AS CreationDate
  , CONCAT(
    LPAD(EXTRACT(HOUR FROM SPLIT(CreationTime, 'T')[1]), 2, '0'), 
    LPAD(EXTRACT(MINUTE FROM SPLIT(CreationTime, 'T')[1]), 2, '0')
    ) AS CreationTimeId
  FROM activityEventsWObjects
)

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Overwrite and Optimize Tables

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Write & Optimize Dimensions

-- COMMAND ----------

-- MAGIC %python
-- MAGIC import time
-- MAGIC
-- MAGIC dims = ['dimApps', 'dimCapacities', 'dimDashboards', 'dimDataflows', 'dimDataflowGen2s', 'dimDatasets', 'dimDatasources', 'dimDatawarehouses', 'dimDefaultWarehouses', 'dimEventStreams', 'dimImports', 'dimLakehouses', 'dimLakewarehouses', 'dimModels', 'dimPipelines', 'dimReports', 'dimUsers', 'dimWorkspaces', 'dimDates']
-- MAGIC
-- MAGIC for table in dims:
-- MAGIC     writePath = f'{writePathRoot}_{table}/delta'
-- MAGIC     start = time.time()
-- MAGIC     overwriteDeltaTable(toDf(table).withColumnRenamed('CreationTime', 'LastAccess'), writePath)
-- MAGIC     end = time.time()
-- MAGIC     dif = round(end - start, 2)
-- MAGIC     print(f'{table} took {dif} seconds')

-- COMMAND ----------

-- MAGIC %python
-- MAGIC for table in dims:
-- MAGIC     tableName = f'{topic}_{table}'
-- MAGIC     writePath = f'{writePathRoot}_{table}/delta'
-- MAGIC     start = time.time()
-- MAGIC     sql_optimize_vacuum(database, tableName, writePath)
-- MAGIC     end = time.time()
-- MAGIC     dif = round(end - start, 2)
-- MAGIC     print(f'{table} took {dif} seconds')

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Write & Optimize Fact

-- COMMAND ----------

-- MAGIC %python
-- MAGIC
-- MAGIC factWritePath = f'{writePathRoot}_fact/delta' ### REMOVE the /delta don't need to store data in this folder.   ADD a version number to the folder path
-- MAGIC overwriteDeltaTable(toDf('fact'), factWritePath)

-- COMMAND ----------

-- MAGIC %python
-- MAGIC
-- MAGIC sql_optimize_vacuum(database, f'{topic}_fact', factWritePath)

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Drop Views

-- COMMAND ----------

-- MAGIC %python
-- MAGIC for table in dims:
-- MAGIC     start = time.time()
-- MAGIC     spark.sql(f'DROP VIEW {table}')
-- MAGIC     end = time.time()
-- MAGIC     dif = round(end - start, 2)
-- MAGIC     print(f'{table} took {dif} seconds')

-- COMMAND ----------

DROP VIEW activityEventsTemp;
DROP VIEW activityEventsWObjects;
DROP VIEW workspacesDupes;
DROP VIEW reportsDupes;
DROP VIEW fact;
