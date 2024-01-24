-- Databricks notebook source
-- MAGIC %md 
-- MAGIC ### Scan Result Workspaces
-- MAGIC

-- COMMAND ----------

CREATE STREAMING TABLE scanResultWorkspaces_raw 
TBLPROPERTIES ("quality" = "bronze")
AS
SELECT 
   json.id AS workspaceId
  , json.* EXCEPT (id, users)
  , json.users AS workspaceUsers
  , processedTimeStamp
  , processedDate
  , TenantId
    FROM STREAM read_files("/Volumes/pbimonitor/base/files/scan_result/*",  multiLine=> true, schema => '
  STRUCT<
    workspaces: ARRAY<
      STRUCT<
        id: STRING,
        name: STRING,
        description: STRING,
        type: STRING,
        state: STRING,
        isOnDedicatedCapacity: STRING,
        capacityId: STRING,
        defaultDatasetStorageFormat: STRING,
        eventStream: STRING,
        reports: ARRAY<
          STRUCT<
            reportType: STRING,
            id: STRING,
            name: STRING,
            datasetId: STRING,
            modifiedDateTime: STRING,
            originalReportObjectId: STRING,
            modifiedBy: STRING,
            modifiedById: STRING,
            users: ARRAY<
              STRUCT<
                reportUserAccessRight: STRING,
                emailAddress: STRING,
                displayName: STRING,
                identifier: STRING,
                graphId: STRING,
                principalType: STRING,
                userType: STRING
              >
            >
          >
        >,
        dashboards: ARRAY<
          STRUCT<
            id: STRING,
            displayName: STRING,
            isReadOnly: STRING,
            tiles: ARRAY<
              STRUCT<
                id: STRING,
                reportId: STRING,
                datasetId: STRING
              >
            >,
            users: ARRAY<
              STRUCT<
                dashboardUserAccessRight: STRING,
                emailAddress: STRING,
                displayName: STRING,
                identifier: STRING,
                graphId: STRING,
                principalType: STRING,
                userType: STRING
              >
            >
          >
        >,
        datasets: ARRAY<
          STRUCT<
            id: STRING,
            name: STRING,
            tables: ARRAY<
              STRUCT<
                name: STRING,
                columns: ARRAY<
                  STRUCT<
                    name: STRING,
                    dataType: STRING,
                    expression: STRING,
                    isHidden: STRING
                  >
                >,
                measures: ARRAY<
                  STRUCT<
                    name: STRING,
                    expression: STRING,
                    isHidden: STRING
                  >
                >,
                isHidden: STRING,
                source: ARRAY<
                  STRUCT<
                    expression: STRING
                  >
                >
              >
            >,
            expressions: ARRAY<
              STRUCT<
                name: STRING,
                description: STRING,
                expression: STRING
              >
            >,
            configuredBy: STRING,
            configuredById: STRING,
            isEffectiveIdentityRequired: STRING,
            isEffectiveIdentityRolesRequired: STRING,
            directQueryRefreshScheule: STRING,
            targetStorageMode: STRING,
            createdDate: STRING,
            schemaRetrievalError: STRING,
            contentProviderType: STRING,
            datasourceUsages: ARRAY<
              STRUCT<
                datasourceInstanceId: STRING
              >
            >,
            misconfiguredDatasourcesUsages: ARRAY<
              STRUCT<
                datasourceInstanceId: STRING
              >
            >,
            users: ARRAY<
              STRUCT<
                datasetUserAccessRight: STRING,
                emailAddress: STRING,
                displayName: STRING,
                identifier: STRING,
                graphId: STRING,
                principalType: STRING,
                userType: STRING
              >
            >
          >
        >,
        dataflows: ARRAY<
          STRUCT<
            objectId: STRING,
            name: STRING,
            description: STRING,
            configuredBy: STRING,
            modifiedBy: STRING,
            modifiedDateTime: STRING,
            datasourceUsages: ARRAY<
              STRUCT<
                datasourceInstanceId: STRING
              >
            >,
            upstreamDataflows: ARRAY<
              STRUCT<
                targetDataflowId: STRING,
                groupId: STRING
              >
            >,
            users: ARRAY<
              STRUCT<
                dataflowUserAccessRight: STRING,
                emailAddress: STRING,
                displayName: STRING,
                identifier: STRING,
                graphId: STRING,
                principalType: STRING,
                userType: STRING
              >
            >
          >
        >,
        datamarts: ARRAY<
          STRUCT<
            id: STRING,
            name: STRING,
            type: STRING,
            configuredBy: STRING,
            configuredById: STRING,
            modifiedBy: STRING,
            modifiedById: STRING,
            modifiedDateTime: STRING,
            refreshSchedule: STRUCT<
              days: STRING,
              times: STRING,
              enabled: STRING,
              localTimeZoneId: STRING,
              notifyOption: STRING
            >,
            datasourceUsages: ARRAY<
              STRUCT<
                datasourceInstanceId: STRING
              >
            >,
            users: ARRAY<
              STRUCT<
                datamartUserAccessRight: STRING,
                emailAddress: STRING,
                displayName: STRING,
                identifier: STRING,
                graphId: STRING,
                principalType: STRING,
                userType: STRING
              >
            >
          >
        >,
        users: ARRAY<
          STRUCT<
            groupUserAccessRight: STRING,
            emailAddress: STRING,
            displayName: STRING,
            identifier: STRING,
            graphId: STRING,
            principalType: STRING,
            userType: STRING
          >
        >
      >
    >,
    processedTimeStamp: STRING,
    processedDate: STRING,
    TenantId: STRING
  >
'
)
LATERAL VIEW explode(workspaces) AS json



-- COMMAND ----------

-- MAGIC %md 
-- MAGIC # NEEDS: more fields from json to schema (fabric)

-- COMMAND ----------

-- MAGIC %md
-- MAGIC Unique Key = `workspaceId`\
-- MAGIC **Review:** This may result in undesired behavior as I believe deleted workspaces will be kept in table (good) but deleted or removed sub-artifacts (reports, datasets, etc.) will not be kept

-- COMMAND ----------

CREATE STREAMING TABLE scanResultWorkspaces_current 

-- COMMAND ----------

APPLY CHANGES INTO LIVE.scanResultWorkspaces_current
FROM STREAM(LIVE.scanResultWorkspaces_raw)
KEYS(workspaceId)
SEQUENCE BY processedTimeStamp

-- COMMAND ----------

-- MAGIC %md 
-- MAGIC ### Scan Result Datasources

-- COMMAND ----------

CREATE STREAMING TABLE scanResultDatasourceInstances_raw 
TBLPROPERTIES ("quality" = "bronze")
AS
SELECT 
  json.* EXCEPT (connectionDetails)
  , json.connectionDetails.*
  , processedTimeStamp
  , processedDate
  , TenantId
    FROM STREAM read_files("/Volumes/pbimonitor/base/files/scan_result/*",  multiLine=> true, schema => '
  STRUCT<
     datasourceInstances: ARRAY<
      STRUCT<
        datasourceType: STRING,
        connectionDetails: STRUCT<
          url: STRING,
          sharePointSiteUrl: STRING,
          path: STRING,
          server: STRING,
          database: STRING,
          extensionDataSourceKind: STRING,
          extensionDataSourcePath: STRING
        >,
        datasourceId: STRING,
        gatewayId: STRING
      >
    >,
    processedTimeStamp: STRING,
    processedDate: STRING,
    TenantId: STRING
  >
'
)
  LATERAL VIEW explode(datasourceInstances) AS json



-- COMMAND ----------

-- MAGIC %md
-- MAGIC Unique Key = `datasourceId`

-- COMMAND ----------

CREATE STREAMING TABLE scanResultDatasourceInstances_current 

-- COMMAND ----------

APPLY CHANGES INTO LIVE.scanResultDatasourceInstances_current
FROM STREAM(LIVE.scanResultDatasourceInstances_raw)
KEYS(datasourceId)
SEQUENCE BY processedTimeStamp

-- COMMAND ----------

-- MAGIC %md 
-- MAGIC ### Scan Result Misconfigured Datasource Instances

-- COMMAND ----------

CREATE STREAMING TABLE scanResultMisconfiguredDatasourceInstances_raw 
TBLPROPERTIES ("quality" = "bronze")
AS
SELECT 
json.* EXCEPT (connectionDetails)
  , json.connectionDetails.*
  , processedTimeStamp
  , TenantId
    FROM STREAM read_files("/Volumes/pbimonitor/base/files/scan_result/*",  multiLine=> true, schema => '
  STRUCT<
   misconfiguredDatasourceInstances: ARRAY<
      STRUCT<
        datasourceType: STRING,
        connectionDetails: STRUCT<
          url: STRING,
          sharePointSiteUrl: STRING,
          path: STRING,
          server: STRING,
          database: STRING,
          extensionDataSourceKind: STRING,
          extensionDataSourcePath: STRING
        >,
        datasourceId: STRING
      >
    >,
    processedTimeStamp: STRING,
    processedDate: STRING,
    TenantId: STRING
  >
'
)
  LATERAL VIEW explode(misconfiguredDatasourceInstances) AS json



-- COMMAND ----------

-- MAGIC %md
-- MAGIC Unique Key = `datasourceId`

-- COMMAND ----------

CREATE STREAMING TABLE scanResultMisconfiguredDatasourceInstances_current 

-- COMMAND ----------

APPLY CHANGES INTO LIVE.scanResultMisconfiguredDatasourceInstances_current
FROM STREAM(LIVE.scanResultMisconfiguredDatasourceInstances_raw)
KEYS(datasourceId)
SEQUENCE BY processedTimeStamp

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ####Workspaces

-- COMMAND ----------

CREATE LIVE VIEW workspaces_catalog
AS  
  SELECT workspaceId
  , name AS workspaceName
  , description
  , type
  , state
  , isOnDedicatedCapacity
  , workspaceUsers AS users
  , TenantId
  FROM LIVE.scanResultWorkspaces_current


-- COMMAND ----------

CREATE OR REFRESH LIVE TABLE dimWorkspaces_catalog
AS 
SELECT * EXCEPT (users)
  FROM LIVE.workspaces_catalog


-- COMMAND ----------

-- MAGIC %md
-- MAGIC ####Reports

-- COMMAND ----------

CREATE LIVE VIEW reports_catalog
AS 
SELECT 
    workspaceId 
    ,json.id AS reportId
    ,json.* EXCEPT (id)
    , TenantId
  FROM LIVE.scanResultWorkspaces_current
  lateral view explode(reports) as json


-- COMMAND ----------

CREATE OR REFRESH LIVE TABLE dimReports_catalog
AS 
  SELECT * EXCEPT (users)
  FROM LIVE.reports_catalog


-- COMMAND ----------

-- MAGIC %md
-- MAGIC ####Dashboards

-- COMMAND ----------

CREATE LIVE VIEW dashboards_catalog
AS (
  SELECT workspaceId
  , json.id AS dashboardId
  , json.displayName AS dashboardName
  , json.* EXCEPT (id, displayName)
  , TenantId
  FROM LIVE.scanResultWorkspaces_current
  LATERAL VIEW explode(dashboards) AS json
)


-- COMMAND ----------

CREATE OR REFRESH LIVE TABLE dimDashboards_catalog
AS (
  SELECT * EXCEPT (Tiles, users)
  FROM LIVE.dashboards_catalog
)

-- COMMAND ----------

CREATE OR REFRESH LIVE TABLE dimTiles_catalog
AS (
  SELECT dashboardId
  , json.*
  , TenantId
  FROM LIVE.dashboards_catalog
  LATERAL VIEW explode(tiles) AS json
)

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #### Datasets

-- COMMAND ----------

CREATE LIVE VIEW datasets_catalog
AS (
  SELECT workspaceId
  , json.id AS datasetId
  , json.name AS datasetName
  , json.* EXCEPT (name, id)
  , TenantId
  FROM LIVE.scanResultWorkspaces_current
  LATERAL VIEW explode(datasets) AS json
)

-- COMMAND ----------

CREATE OR REFRESH LIVE TABLE dimDatasets_catalog
AS (
  SELECT * EXCEPT (Expressions, Tables, users)
  FROM LIVE.datasets_catalog
)

-- COMMAND ----------

CREATE OR REFRESH LIVE TABLE factDatasourceUsages_catalog
AS (
  SELECT datasetId
  , json.*
  FROM LIVE.datasets_catalog
  LATERAL VIEW EXPLODE (datasourceUsages) AS json
)

-- COMMAND ----------

CREATE OR REFRESH LIVE TABLE dimExpressions_catalog
AS (
  SELECT DatasetId
  , json.*
  , TenantId
  FROM LIVE.datasets_catalog
  LATERAL VIEW explode(Expressions) AS json
)

-- COMMAND ----------

CREATE LIVE VIEW tables_catalog
AS (
  SELECT DatasetId
  , json.*
  , TenantId
  FROM LIVE.datasets_catalog
  LATERAL VIEW EXPLODE (Tables) AS json
)

-- COMMAND ----------

CREATE OR REFRESH LIVE TABLE dimTables_catalog
AS (
  SELECT * EXCEPT (columns, measures, name)
  , name AS tableName
  FROM LIVE.tables_catalog
)

-- COMMAND ----------

CREATE OR REFRESH LIVE TABLE dimColumns_catalog
AS (
  SELECT DatasetId
  , json.* EXCEPT (name)
  , name AS columnName
  , TenantId
  FROM LIVE.tables_catalog
  LATERAL VIEW EXPLODE(columns) AS json
)

-- COMMAND ----------

CREATE OR REFRESH LIVE TABLE dimMeasures_catalog
AS (
  SELECT DatasetId
  , name AS tableName
  , json.*
  FROM LIVE.tables_catalog
  LATERAL VIEW EXPLODE(measures) AS json
)

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #### Dataflows

-- COMMAND ----------

CREATE LIVE VIEW dataflows_catalog
AS (
  SELECT workspaceId
  , json.objectId AS dataflowId
  , json.name AS dataflowName
  , json.* EXCEPT (objectId, name)
  , TenantId
  FROM LIVE.scanResultWorkspaces_current
  LATERAL VIEW explode(dataflows) AS json
)

-- COMMAND ----------

CREATE OR REFRESH LIVE TABLE dimDataflows_catalog
AS (
  SELECT * EXCEPT (datasourceUsages, upstreamDataflows, users)
  FROM LIVE.dataflows_catalog
)

-- COMMAND ----------

CREATE OR REFRESH LIVE TABLE dimDataflowSourceUsages_catalog
AS (
  SELECT dataflowId 
  , json.*
  , TenantId
  FROM LIVE.dataflows_catalog
  LATERAL VIEW explode(datasourceUsages) AS json
)

-- COMMAND ----------

CREATE OR REFRESH LIVE TABLE dimDataflowsUpstream_catalog
AS (
  SELECT dataflowId 
  , json.*
  , TenantId
  FROM LIVE.dataflows_catalog
  LATERAL VIEW explode(upstreamDataflows) AS json
)

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #### Datamarts

-- COMMAND ----------

CREATE LIVE VIEW datamarts_catalog
AS (
  SELECT workspaceId
  , json.id AS datamartId
  , json.name AS datamartName
  , json.* EXCEPT (name, id)
  , TenantId
  FROM LIVE.scanResultWorkspaces_current
  LATERAL VIEW explode(datamarts) AS json
)

-- COMMAND ----------

CREATE OR REFRESH LIVE TABLE dimDatamarts_catalog
AS (
  SELECT * EXCEPT (datasourceUsages, users)
  FROM LIVE.datamarts_catalog
)

-- COMMAND ----------

CREATE OR REFRESH LIVE TABLE dimDatamartSourceUsages_catalog
AS (
  SELECT datamartId
  , json.*
  , TenantId
  FROM LIVE.datamarts_catalog
  LATERAL VIEW explode(datasourceUsages) AS json
)

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #### Artifact Users

-- COMMAND ----------

CREATE OR REFRESH LIVE TABLE dimWorkspaceUsers_catalog
AS (
  SELECT workspaceId
        , json.*
        , TenantId
        FROM LIVE.workspaces_catalog
        LATERAL VIEW explode(users) AS json
)

-- COMMAND ----------

CREATE OR REFRESH LIVE TABLE dimReportUsers_catalog
AS (
  SELECT reportId
        , json.*
        , TenantId
        FROM LIVE.reports_catalog
        LATERAL VIEW explode(users) AS json
)

-- COMMAND ----------

CREATE OR REFRESH LIVE TABLE dimDashboardUsers_catalog
AS (
  SELECT dashboardId
        , json.*
        , TenantId
        FROM LIVE.dashboards_catalog
        LATERAL VIEW explode(users) AS json
)

-- COMMAND ----------

CREATE OR REFRESH LIVE TABLE dimDatasetUsers_catalog
AS (
  SELECT datasetId
        , json.*
        , TenantId
        FROM LIVE.datasets_catalog
        LATERAL VIEW explode(users) AS json
)

-- COMMAND ----------

CREATE OR REFRESH LIVE TABLE dimDataflowUsers_catalog
AS (
  SELECT dataflowId
        , json.*
        , TenantId
        FROM LIVE.dataflows_catalog
        LATERAL VIEW explode(users) AS json
)

-- COMMAND ----------

CREATE OR REFRESH LIVE TABLE dimDatamartUsers_catalog
AS (
  SELECT datamartId
        , json.*
        , TenantId
        FROM LIVE.datamarts_catalog
        LATERAL VIEW explode(users) AS json
)
