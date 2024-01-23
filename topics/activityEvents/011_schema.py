# Databricks notebook source
# MAGIC %md
# MAGIC #getActivityEvents Schema

# COMMAND ----------

# MAGIC %md
# MAGIC ### Full Schema written in levels 
# MAGIC Commented out fields are present in the data but not needed in the table

# COMMAND ----------

from pyspark.sql.types import *

# Inner object for Entities
activityEventEntities = StructType(
    [
        StructField('Id',StringType(),True),
        StructField('RecordType',StringType(),True),
        StructField('CreationTime',StringType(),True),
        StructField('Operation',StringType(),True),
        StructField('Activity',StringType(),True),
        StructField('ActivityId',StringType(),True),
        StructField('OrganizationId',StringType(),True),
        StructField('UserType',StringType(),True),
        StructField('UserKey',StringType(),True),
        StructField('UserId',StringType(),True),
        StructField('UserAgent',StringType(),True),
        StructField('ClientIP',StringType(),True),
        StructField('Workload',StringType(),True),
        StructField('ResultStatus',StringType(),True),
        StructField('IsSuccess',StringType(),True),
        StructField('FolderObjectId',StringType(),True),
        StructField('FolderDisplayName',StringType(),True),
        StructField('FolderAccessRequests',StringType(),True),
        StructField('ArtifactName',StringType(),True),
        StructField('ArtifactId',StringType(),True),
        StructField('ArtifactKind',StringType(),True),
        StructField('AuditedArtifactInformation',StringType(),True),
        StructField('ItemName',StringType(),True),
        StructField('ObjectId',StringType(),True),
        StructField('ObjectType',StringType(),True),
        StructField('ObjectDisplayName',StringType(),True),
        StructField('WorkSpaceName',StringType(),True),
        StructField('WorkspaceId',StringType(),True),
        StructField('WorkspacesSemicolonDelimitedList',StringType(),True),
        StructField('TargetWorkspaceId',StringType(),True),
        StructField('DatasetName',StringType(),True),
        StructField('DatasetId',StringType(),True),
        StructField('Datasets',StringType(),True),
        StructField('ReportName',StringType(),True),
        StructField('ReportId',StringType(),True),
        StructField('ReportType',StringType(),True),
        StructField('CopiedReportId',StringType(),True),
        StructField('CopiedReportName',StringType(),True),
        StructField('DashboardId',StringType(),True),
        StructField('DashboardName',StringType(),True),
        StructField('TileText',StringType(),True),
        StructField('AppName',StringType(),True),
        StructField('AppId',StringType(),True),
        StructField('AppReportId',StringType(),True),
        StructField('IsTemplateAppFromMarketplace',StringType(),True),
        StructField('TemplateAppFolderObjectId',StringType(),True),
        StructField('TemplateAppIsInstalledWithAutomation',StringType(),True),
        StructField('TemplateAppObjectId',StringType(),True),
        StructField('TemplateAppPackageObjectId',StringType(),True),
        StructField('TemplateAppPackageSourceStage',StringType(),True),
        StructField('TemplateAppOwnerTenantObjectId',StringType(),True),
        StructField('TemplateAppVersion',StringType(),True),
        StructField('TemplatePackageName',StringType(),True),
        StructField('IsUpdateAppActivity',StringType(),True),
        StructField('OrgAppPermission',StringType(),True),
        StructField('DataflowName',StringType(),True),
        StructField('DataflowId',StringType(),True),
        StructField('DataflowType',StringType(),True),
        StructField('DataflowRefreshScheduleType',StringType(),True),
        StructField('DataflowAccessTokenRequestParameters',StringType(),True),
        StructField('DataflowAllowNativeQueries',StringType(),True),
        StructField('DatasourceId',StringType(),True),
        StructField('DatasourceDetails',StringType(),True),
        StructField('PaginatedReportDataSources',StringType(),True),
        StructField('CapacityName',StringType(),True),
        StructField('CapacityId',StringType(),True),
        StructField('CapacityState',StringType(),True),
        StructField('CapacityUsers',StringType(),True),
        StructField('DeploymentPipelineId',StringType(),True),
        StructField('DeploymentPipelineAccesses',StringType(),True),
        StructField('DeploymentPipelineDisplayName',StringType(),True),
        StructField('GatewayId',StringType(),True),
        StructField('EmbedTokenId',StringType(),True),
        StructField('RequestId',StringType(),True),
        StructField('Experience',StringType(),True),
        StructField('DistributionMethod',StringType(),True),
        StructField('ConsumptionMethod',StringType(),True),
        StructField('DataConnectivityModel',StringType(),True),
        StructField('DataConnectivityMode',StringType(),True),
        StructField('RefreshType',StringType(),True),
        StructField('LastRefreshTime',StringType(),True),
        StructField('RequiredWorkspacess',StringType(),True),
        StructField('IncludeSubartifacts',StringType(),True),
        StructField('IncludeExpressions',StringType(),True),
        StructField('Lineage',StringType(),True),
        StructField('ModelsSnapshots',StringType(),True),
        StructField('ExcludePersonalWorkspaces',StringType(),True),
        StructField('RequiredWorkspaces',StringType(),True),
        StructField('ItemsCount',StringType(),True),
        StructField('DeploymentPipelineObjectId',StringType(),True),
        StructField('ExportEventStartDateTimeParameter',StringType(),True),
        StructField('ExportEventEndDateTimeParameter',StringType(),True),
        StructField('GitIntegrationRequest',StringType(),True),
        StructField('ImportDisplayName',StringType(),True),
        StructField('ImportId',StringType(),True),
        StructField('ImportSource',StringType(),True),
        StructField('ImportType',StringType(),True),
        StructField('ExportedArtifactInfo',StringType(),True),
        StructField('OriginalOwner',StringType(),True),
        StructField('WorkspaceAccessList',StringType(),True),
        StructField('AggregatedWorkspaceInformation',StringType(),True),
        StructField('InPlaceSharingEnabled',StringType(),True),
        StructField('TakingOverOwner',StringType(),True),
        StructField('Schedules',StringType(),True),
        StructField('ShareLinkId',StringType(),True),
        StructField('SharingAction',StringType(),True),
        StructField('SharingInformation',StringType(),True),
        StructField('SharingScope',StringType(),True),
        StructField('SwitchState',StringType(),True),
        StructField('ExternalSubscribeeInformation',StringType(),True),
        StructField('SubscribeeInformation',StringType(),True),
        StructField('SubscriptionSchedule',StringType(),True),
        StructField('HasFullReportAttachment',StringType(),True),
        StructField('ItemNewName',StringType(),True),
        StructField('AccessRequestMessage',StringType(),True),
        StructField('AccessRequestType',StringType(),True),
    ]
)

#Level 0 Keys
activityEventSchema = StructType(
    [
        StructField('activityEventEntities',    ArrayType(activityEventEntities),True),
        #StructField('continuationUri',         StringType(),True),
        #StructField('continuationToken',       StringType(),True),
        #StructField('lastResultSet',           StringType(),True),
        StructField('activityDate',             StringType(),True),
        StructField('processedTimeStamp',       StringType(),True),
        StructField('processedDate',            StringType(),True)
    ]
)

# COMMAND ----------


