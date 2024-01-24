-- Databricks notebook source
CREATE STREAMING TABLE activities_raw
TBLPROPERTIES ("quality" = "bronze")
AS
SELECT 
      activityEvents.*,
      processedTimeStamp,
      activityDate,
      TenantId
      
FROM STREAM read_files("/Volumes/pbimonitor/base/files/activityEvents/*", multiLine => true, schema => '
  STRUCT<
    activityDate: STRING,
    activityEventEntities: ARRAY<
      STRUCT<
        AppName: STRING,
        AppId: STRING,
        AppReportId: STRING,
        Activity: STRING,
        ActivityId: STRING,
        ArtifactId: STRING,
        ArtifactKind: STRING,
        ArtifactName: STRING,
        AuditedArtifactInformation: STRING,
        CapacityId: STRING,
        CapacityName: STRING,
        CapacityState: STRING,
        CapacityUsers: STRING,
        ClientIP: STRING,
        ConsumptionMethod: STRING,
        CopiedReportId: STRING,
        CopiedReportName: STRING,
        CreationTime: STRING,
        DashboardId: STRING,
        DashboardName: STRING,
        DatasetId: STRING,
        DatasetName: STRING,
        Datasets: STRING,
        DataflowName: STRING,
        DataflowId: STRING,
        DataflowType: STRING,
        DataflowRefreshScheduleType: STRING,
        DataflowAccessTokenRequestParameters: STRING,
        DataflowAllowNativeQueries: STRING,
        DatasourceId: STRING,
        DatasourceDetails: STRING,
        DistributionMethod: STRING,
        EmbedTokenId: STRING,
        FolderObjectId: STRING,
        FolderDisplayName: STRING,
        FolderAccessRequests: STRING,
        Id: STRING,
        IsSuccess: STRING, 
        IsUpdateAppActivity: STRING,
        ItemName: STRING,
        IsTemplateAppFromMarketplace: STRING,
        ObjectId: STRING,
        ObjectType: STRING,
        ObjectDisplayName: STRING,
        Operation: STRING,
        OrganizationId: STRING,
        OrgAppPermission: STRING,
        PaginatedReportDataSources: STRING,
        RecordType: STRING,
        RefreshEnforcementPolicy: STRING, 
        ReportId: STRING,
        ReportName: STRING,
        ReportType: STRING,
        RequestId: STRING,
        ResultStatus: STRING,
        TargetWorkspaceId: STRING,
        TemplateAppFolderObjectId: STRING,
        TemplateAppIsInstalledWithAutomation: STRING,
        TemplateAppObjectId: STRING,
        TemplateAppPackageObjectId: STRING,
        TemplateAppPackageSourceStage: STRING,
        TemplateAppOwnerTenantObjectId: STRING,
        TemplateAppVersion: STRING,
        TemplatePackageName: STRING,
        TileText: STRING,
        UserAgent: STRING,
        UserId: STRING,
        UserKey: STRING,
        UserType: STRING, 
        Workload: STRING,
        WorkSpaceName: STRING,
        WorkspacesSemicolonDelimitedList: STRING,
        WorkspaceId: STRING,
        DeploymentPipelineId: STRING,
        DeploymentPipelineAccesses: STRING,
        DeploymentPipelineDisplayName: STRING,
        GatewayId: STRING,
        Experience: STRING,
        DataConnectivityModel: STRING,
        DataConnectivityMode: STRING,
        RefreshType: STRING,
        LastRefreshTime: STRING,
        RequiredWorkspacess: STRING,
        IncludeSubartifacts: STRING,
        IncludeExpressions: STRING,
        Lineage: STRING,
        ModelsSnapshots: STRING,
        ExcludePersonalWorkspaces: STRING,
        ItemsCount: STRING,
        DeploymentPipelineObjectId: STRING,
        ExportEventStartDateTimeParameter: STRING,
        ExportEventEndDateTimeParameter: STRING,
        GitIntegrationRequest: STRING,
        ImportDisplayName: STRING,
        ImportId: STRING,
        ImportSource: STRING,
        ImportType: STRING,
        ExportedArtifactInfo: STRING,
        OriginalOwner: STRING,
        WorkspaceAccessList: STRING,
        AggregatedWorkspaceInformation: STRING,
        InPlaceSharingEnabled: STRING,
        TakingOverOwner: STRING,
        Schedules: STRING,
        ShareLinkId: STRING,
        SharingAction: STRING,
        SharingInformation: STRING,
        SharingScope: STRING,
        SwitchState: STRING,
        ExternalSubscribeeInformation: STRING,
        SubscribeeInformation: STRING,
        SubscriptionSchedule: STRING,
        HasFullReportAttachment: STRING,
        ItemNewName: STRING,
        AccessRequestMessage: STRING,
        AccessRequestType: STRING
      >
    >, 
    lastResultSet: STRING, 
    processedDate: STRING,
    processedTimeStamp: STRING,
    TenantId: STRING
  >
')
LATERAL VIEW explode(activityEventEntities) AS activityEvents


-- COMMAND ----------

-- MAGIC %md
-- MAGIC Unique Key = `id` (different from activityId)

-- COMMAND ----------

CREATE STREAMING TABLE activities_current 

-- COMMAND ----------

APPLY CHANGES INTO LIVE.activities_current
FROM STREAM(LIVE.activities_raw)
KEYS(id)
SEQUENCE BY processedTimeStamp

-- COMMAND ----------

CREATE LIVE VIEW activityEventsWObjects
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
  FROM LIVE.activities_current
)

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Dimensions
-- MAGIC
-- MAGIC Eventually we are going to combine dimension tables where it makes sense, for example, there is a dimUsers in activitiyEvents as well as msGraph and capacities

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Users

-- COMMAND ----------

CREATE OR REFRESH LIVE TABLE dimUsers_activities
AS (
  WITH grouped AS (
    SELECT UserId, MAX(CreationTime) AS CreationTime
    FROM LIVE.activityEventsWObjects
    WHERE UserId IS NOT NULL
    GROUP BY UserId
  )

  SELECT DISTINCT UserId, CreationTime, TenantId
  FROM grouped
  LEFT JOIN LIVE.activityEventsWObjects USING (UserId, CreationTime)
)

-- COMMAND ----------

-- MAGIC  %md
-- MAGIC ### Workspaces
-- MAGIC
-- MAGIC  There are two columns that mean the same thing. WorkspaceId column is the same as FolderObjectId 
-- MAGIC  This query will merge both columns for Id and Name down to one complete list of Workspaces
-- MAGIC
-- MAGIC  ```
-- MAGIC  Folder = Workspace
-- MAGIC  ```
-- MAGIC
-- MAGIC

-- COMMAND ----------

CREATE LIVE VIEW workspacesDupes
AS (
    SELECT WorkspaceId
    , WorkspaceName AS Name
    , CreationTime
    , TenantId
    FROM LIVE.activityEventsWObjects
    WHERE WorkspaceName <> "Unkown"

    UNION 
    
    SELECT FolderObjectId AS WorkspaceId
    , FolderDisplayName AS Name
    , CreationTime
    , TenantId
    FROM LIVE.activityEventsWObjects
)


-- COMMAND ----------

-- MAGIC %md
-- MAGIC > **Note:** When data is added with the braces `{}` indicate that data was created from the ETL process.
-- MAGIC
-- MAGIC

-- COMMAND ----------

CREATE OR REFRESH LIVE TABLE dimWorkspaces_activities
AS (
  WITH grouped AS (
    SELECT WorkspaceId, MAX(CreationTime) AS CreationTime
    FROM LIVE.workspacesDupes
    WHERE WorkspaceId IS NOT NULL AND Name IS NOT NULL
    GROUP BY WorkspaceId
  )

  SELECT WorkspaceId,
  CASE 
    WHEN WorkspaceId = '00000000-0000-0000-0000-000000000000' THEN '{0}' 
    ELSE Name
  END AS WorkspaceName
  , CreationTime
  , TenantId
  FROM grouped
  LEFT JOIN (
    SELECT *
    FROM LIVE.workspacesDupes 
    WHERE Name IS NOT NULL
  )
  USING (WorkspaceId, CreationTime)
)

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Datasets
-- MAGIC

-- COMMAND ----------

CREATE OR REFRESH LIVE TABLE dimDatasets_activities
AS (
  WITH grouped AS (
    SELECT DatasetId, MAX(CreationTime) AS CreationTime
    FROM LIVE.activityEventsWObjects
    WHERE DatasetId IS NOT NULL AND DatasetName IS NOT NULL
    GROUP BY DatasetId
  )

  SELECT DISTINCT DatasetId, DatasetName, CreationTime, TenantId
  FROM grouped
  LEFT JOIN LIVE.activityEventsWObjects USING (DatasetId, CreationTime)
  WHERE DatasetName <> "Unkownn"
)

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Reports
-- MAGIC

-- COMMAND ----------

CREATE LIVE VIEW reportsDupes
AS (
    SELECT ReportId
    , ReportName AS ReportName
    , ReportType
    , CreationTime
    , TenantId
    FROM LIVE.activityEventsWObjects
    
    UNION 
    
    SELECT CopiedReportId AS ReportId
    , CopiedReportName AS ReportName
    , ReportType
    , CreationTime
    , TenantId
    FROM LIVE.activityEventsWObjects
)

-- COMMAND ----------

CREATE OR REFRESH LIVE TABLE dimReports_activities
AS (
  WITH grouped AS (
    SELECT ReportId, MAX(CreationTime) AS CreationTime
    FROM LIVE.reportsDupes
    WHERE ReportId IS NOT NULL AND ReportName IS NOT NULL
    GROUP BY ReportId
  )

  SELECT DISTINCT ReportId, ReportName, ReportType, CreationTime, TenantId
  FROM grouped
  LEFT JOIN LIVE.activityEventsWObjects USING (ReportId, CreationTime)
  WHERE ReportName != "Unkown"
)

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Dashboards
-- MAGIC

-- COMMAND ----------

CREATE OR REFRESH LIVE TABLE dimDashboards_activities
AS (
  WITH grouped AS (
    SELECT DashboardId, MAX(CreationTime) AS CreationTime
    FROM LIVE.activityEventsWObjects
    WHERE DashboardId IS NOT NULL AND DashboardName IS NOT NULL
    GROUP BY DashboardId
  )

  SELECT DISTINCT DashboardId, DashboardName, CreationTime, TenantId
  FROM grouped
  LEFT JOIN LIVE.activityEventsWObjects USING (DashboardId, CreationTime)
  WHERE DashboardName <> "Unkown"
)

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Apps

-- COMMAND ----------

CREATE OR REFRESH LIVE TABLE dimApps_activities
AS (
  WITH grouped AS (
    SELECT AppId, MAX(CreationTime) AS CreationTime
    FROM LIVE.activityEventsWObjects
    WHERE AppId IS NOT NULL
    GROUP BY AppId
  )

  SELECT DISTINCT AppId, AppName, AppReportId, CreationTime, TenantId
  FROM grouped
  LEFT JOIN LIVE.activityEventsWObjects USING (AppId, CreationTime)
  WHERE AppName <> "Unkown"
)

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Dataflows 
-- MAGIC

-- COMMAND ----------

CREATE OR REFRESH LIVE TABLE dimDataflows_activities
AS (
  WITH grouped AS (
    SELECT DataflowId, MAX(CreationTime) AS CreationTime
    FROM LIVE.activityEventsWObjects
    WHERE DataflowId IS NOT NULL
    GROUP BY DataflowId
  )

  SELECT DISTINCT DataflowId, DataflowName, DataflowType, DataflowRefreshScheduleType, CreationTime, TenantId
  FROM grouped
  LEFT JOIN LIVE.activityEventsWObjects USING (DataflowId, CreationTime)
  WHERE DataflowName <> "Unkown"
)

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Datasources

-- COMMAND ----------

CREATE OR REFRESH LIVE TABLE dimDatasources_activities
AS (
  WITH grouped AS (
    SELECT DatasourceId, MAX(CreationTime) AS CreationTime
    FROM LIVE.activityEventsWObjects
    WHERE DatasourceId IS NOT NULL
    GROUP BY DatasourceId
  )

  SELECT DISTINCT DatasourceId, DatasourceDetails, CreationTime, TenantId
  FROM grouped
  LEFT JOIN LIVE.activityEventsWObjects USING (DatasourceId, CreationTime)
)

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Capacities

-- COMMAND ----------

CREATE OR REFRESH LIVE TABLE dimCapacities_activities
AS (
  WITH grouped AS (
    SELECT CapacityId, MAX(CreationTime) AS CreationTime
    FROM LIVE.activityEventsWObjects
    WHERE CapacityId IS NOT NULL AND CapacityName IS NOT NULL
    GROUP BY CapacityId
  )

  SELECT DISTINCT CapacityId, CapacityName, CapacityState, CreationTime, TenantId
  FROM grouped
  LEFT JOIN LIVE.activityEventsWObjects USING (CapacityId, CreationTime)
  WHERE CapacityName <> "Unknown"
)

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Pipelines

-- COMMAND ----------

CREATE OR REFRESH LIVE TABLE dimPipelines_activities
AS (
  WITH grouped AS (
    SELECT DeploymentPipelineId, MAX(CreationTime) AS CreationTime
    FROM LIVE.activityEventsWObjects
    WHERE DeploymentPipelineId IS NOT NULL
    GROUP BY DeploymentPipelineId
  )

  SELECT DISTINCT DeploymentPipelineId, DeploymentPipelineDisplayName, CreationTime, TenantId
  FROM grouped
  LEFT JOIN LIVE.activityEventsWObjects USING (DeploymentPipelineId, CreationTime)
  WHERE DeploymentPipelineDisplayName <> "Unknown"
)

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Imports

-- COMMAND ----------

CREATE OR REFRESH LIVE TABLE dimImports_activities
AS (
  WITH grouped AS (
    SELECT ImportId, MAX(CreationTime) AS CreationTime
    FROM LIVE.activityEventsWObjects
    WHERE ImportId IS NOT NULL
    GROUP BY importId
  )

  SELECT DISTINCT importId, importDisplayName, importType, importSource, CreationTime, TenantId
  FROM grouped
  LEFT JOIN LIVE.activityEventsWObjects USING (importId, CreationTime)
  WHERE importDisplayName <> "Unknown"
)

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### DataflowGen2s

-- COMMAND ----------

CREATE OR REFRESH LIVE TABLE dimDataflowGen2s_activities
AS (
  WITH grouped AS (
    SELECT DataflowGen2Id, MAX(CreationTime) AS CreationTime
    FROM LIVE.activityEventsWObjects
    WHERE DataflowGen2Id IS NOT NULL
    GROUP BY DataflowGen2Id
  )

  SELECT DISTINCT DataflowGen2Id, DataflowGen2Name, CreationTime, TenantId
  FROM grouped
  LEFT JOIN LIVE.activityEventsWObjects USING (DataflowGen2Id, CreationTime)
  WHERE DataflowGen2Name <> "Unknown"

)


-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### DefaultWarehouses

-- COMMAND ----------

CREATE OR REFRESH LIVE TABLE dimDefaultWarehouses_activities
AS (
  WITH grouped AS (
    SELECT DefaultWarehouseId, MAX(CreationTime) AS CreationTime
    FROM LIVE.activityEventsWObjects
    WHERE DefaultWarehouseId IS NOT NULL
    GROUP BY DefaultWarehouseId
  )

  SELECT DISTINCT DefaultWarehouseId, DefaultWarehouseName, CreationTime, TenantId
  FROM grouped
  LEFT JOIN LIVE.activityEventsWObjects USING (DefaultWarehouseId, CreationTime)
  WHERE DefaultWarehouseName <> "Unknown"

)

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Models

-- COMMAND ----------

CREATE OR REFRESH LIVE TABLE dimModels_activities
AS (
  WITH grouped AS (
    SELECT modelId, MAX(CreationTime) AS CreationTime
    FROM LIVE.activityEventsWObjects
    WHERE modelId IS NOT NULL
    GROUP BY modelId
  )

  SELECT DISTINCT modelId, modelName, CreationTime, TenantId
  FROM grouped
  LEFT JOIN LIVE.activityEventsWObjects USING (modelId, CreationTime)
  WHERE modelName <> "Unknown"

)

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Lakehouses

-- COMMAND ----------

CREATE OR REFRESH LIVE TABLE dimLakehouses_activities
AS (
  WITH grouped AS (
    SELECT lakehouseId, MAX(CreationTime) AS CreationTime
    FROM LIVE.activityEventsWObjects
    WHERE lakehouseId IS NOT NULL
    GROUP BY lakehouseId
  )
  SELECT DISTINCT lakehouseId, lakehouseName, CreationTime, TenantId
  FROM grouped
  LEFT JOIN LIVE.activityEventsWObjects USING (lakehouseId, CreationTime)
  WHERE lakehouseName <> "Unknown"

)

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Datawarehouses

-- COMMAND ----------

CREATE OR REFRESH LIVE TABLE dimDatawarehouses_activities
AS (
  WITH grouped AS (
    SELECT datawarehouseId, MAX(CreationTime) AS CreationTime
    FROM LIVE.activityEventsWObjects
    WHERE datawarehouseId IS NOT NULL
    GROUP BY datawarehouseId
  )

  SELECT DISTINCT datawarehouseId, datawarehouseName, CreationTime, TenantId
  FROM grouped
  LEFT JOIN LIVE.activityEventsWObjects USING (datawarehouseId, CreationTime)
  WHERE datawarehouseName <> "Unknown"
)

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### EventStreams

-- COMMAND ----------

CREATE OR REFRESH LIVE TABLE dimEventStreams_activities
AS (
  WITH grouped AS (
    SELECT eventStreamId, MAX(CreationTime) AS CreationTime
    FROM LIVE.activityEventsWObjects
    WHERE eventStreamId IS NOT NULL
    GROUP BY eventStreamId
  )

  SELECT DISTINCT eventStreamId, eventStreamName, CreationTime, TenantId
  FROM grouped
  LEFT JOIN LIVE.activityEventsWObjects USING (eventStreamId, CreationTime)
  WHERE eventStreamName <> "Unknown"

)

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### LakeWarehouses

-- COMMAND ----------

CREATE OR REFRESH LIVE TABLE dimLakeWarehouses_activities
AS (
  WITH grouped AS (
    SELECT LakeWarehouseId, MAX(CreationTime) AS CreationTime
    FROM LIVE.activityEventsWObjects
    WHERE LakeWarehouseId IS NOT NULL
    GROUP BY LakeWarehouseId
  )

  SELECT DISTINCT LakeWarehouseId, LakeWarehouseName, CreationTime, TenantId
  FROM grouped
  LEFT JOIN LIVE.activityEventsWObjects USING (LakeWarehouseId, CreationTime)
  WHERE LakeWarehouseName <> "Unknown"

)

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Dates

-- COMMAND ----------

CREATE OR REFRESH LIVE TABLE dimDates_activities
AS (
  WITH date_range AS (
    SELECT
      DATE_TRUNC('year', MIN(CreationTime)) AS start_date,
      DATE_TRUNC('year', MAX(CreationTime)) + INTERVAL '2 year' - INTERVAL '1 day' AS end_date
    FROM LIVE.activityEventsWObjects
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


CREATE OR REFRESH LIVE TABLE factActivities
PARTITIONED BY (CreationDate)
AS (
  SELECT AppId, ActivityId, AuditedArtifactInformation, CapacityId, CapacityUsers, ConsumptionMethod, DashboardId, DatasetId, Datasets, DataflowId, DataflowAccessTokenRequestParameters, DataflowAllowNativeQueries, DatasourceId, DistributionMethod, EmbedTokenId, FolderAccessRequests, Id, IsUpdateAppActivity, IsTemplateAppFromMarketplace, Operation, OrgAppPermission, PaginatedReportDataSources, RefreshEnforcementPolicy, ReportId, RequestId, TargetWorkspaceId, TemplateAppFolderObjectId, TemplateAppIsInstalledWithAutomation, TemplateAppObjectId, TemplateAppPackageObjectId, TemplateAppPackageSourceStage, TemplateAppOwnerTenantObjectId, TemplateAppVersion, TemplatePackageName, TileText, UserAgent, UserId, UserKey, UserType, WorkspacesSemicolonDelimitedList, WorkspaceId, DeploymentPipelineId, GatewayId, Experience, DataConnectivityModel, DataConnectivityMode, RefreshType, LastRefreshTime, RequiredWorkspacess, IncludeSubartifacts, IncludeExpressions, Lineage, ModelsSnapshots, ExcludePersonalWorkspaces, ItemsCount, DeploymentPipelineObjectId, ExportEventStartDateTimeParameter, ExportEventEndDateTimeParameter, GitIntegrationRequest, ImportId, ExportedArtifactInfo, OriginalOwner, WorkspaceAccessList, AggregatedWorkspaceInformation, InPlaceSharingEnabled, TakingOverOwner, Schedules, ShareLinkId, SharingAction, SharingInformation, SharingScope, SwitchState, ExternalSubscribeeInformation, SubscribeeInformation, SubscriptionSchedule, HasFullReportAttachment, ItemNewName, AccessRequestMessage, AccessRequestType, TenantId, DataflowGen2Id, DefaultWarehouseId, ModelId, LakehouseId, DatawarehouseId, EventStreamId, LakeWarehouseId
  , COALESCE(ResultStatus, IsSuccess) AS Result
  , SPLIT(CreationTime, 'T')[0] AS CreationDate
  , CONCAT(
    LPAD(EXTRACT(HOUR FROM SPLIT(CreationTime, 'T')[1]), 2, '0'), 
    LPAD(EXTRACT(MINUTE FROM SPLIT(CreationTime, 'T')[1]), 2, '0')
    ) AS CreationTimeId
  FROM LIVE.activityEventsWObjects
)

