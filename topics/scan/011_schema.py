# Databricks notebook source
# MAGIC %md
# MAGIC #Schema for Scan Results

# COMMAND ----------

from pyspark.sql.types import *
# # Inner object for Entities

datasourceUsages = StructType(
    [
        StructField('datasourceInstanceId',StringType(),True)
    ]
)

workspaceUsers = StructType(
    [
        StructField('groupUserAccessRight',StringType(),True),
        StructField('emailAddress',StringType(),True),
        StructField('displayName',StringType(),True),
        StructField('identifier',StringType(),True),
        StructField('graphId',StringType(),True),
        StructField('principalType',StringType(),True),
        StructField('userType',StringType(),True),
    ]
)

datamartUsers = StructType(
    [
        StructField('datamartUserAccessRight',StringType(),True),
        StructField('emailAddress',StringType(),True),
        StructField('displayName',StringType(),True),
        StructField('identifier',StringType(),True),
        StructField('graphId',StringType(),True),
        StructField('principalType',StringType(),True),
        StructField('userType',StringType(),True)
    ]
)

refreshSchedule = StructType(
    [
        StructField('days',StringType(),True),
        StructField('times',StringType(),True),
        StructField('enabled',StringType(),True),
        StructField('localTimeZoneId',StringType(),True),
        StructField('notifyOption',StringType(),True)
    ]
)

datamarts = StructType(
    [
        StructField('id',StringType(),True),
        StructField('name',StringType(),True),
        StructField('type',StringType(),True),
        StructField('configuredBy',StringType(),True),
        StructField('configuredById',StringType(),True),
        StructField('modifiedBy',StringType(),True),
        StructField('modifiedById',StringType(),True),
        StructField('modifiedDateTime',StringType(),True),
        StructField('refreshSchedule',refreshSchedule,True),
        StructField('datasourceUsages',ArrayType(datasourceUsages),True),
        StructField('users',ArrayType(datamartUsers),True)
    ]
)

dataflowUsers = StructType(
    [
        StructField('dataflowUserAccessRight',StringType(),True),
        StructField('emailAddress',StringType(),True),
        StructField('displayName',StringType(),True),
        StructField('identifier',StringType(),True),
        StructField('graphId',StringType(),True),
        StructField('principalType',StringType(),True),
        StructField('userType',StringType(),True)
    ]
)

upstreamDataflows = StructType(
    [
        StructField('targetDataflowId',StringType(),True),
        StructField('groupId',StringType(),True)
    ]
)

dataflows = StructType(
    [
        StructField('objectId',StringType(),True),
        StructField('name',StringType(),True),
        StructField('description',StringType(),True),
        StructField('configuredBy',StringType(),True),
        StructField('modifiedBy',StringType(),True),
        StructField('modifiedDateTime',StringType(),True),
        StructField('datasourceUsages',ArrayType(datasourceUsages),True),
        StructField('upstreamDataflows',ArrayType(upstreamDataflows),True),
        StructField('users',ArrayType(dataflowUsers),True)
    ]
)

datasetUsers = StructType(
    [
        StructField('datasetUserAccessRight',StringType(),True),
        StructField('emailAddress',StringType(),True),
        StructField('displayName',StringType(),True),
        StructField('identifier',StringType(),True),
        StructField('graphId',StringType(),True),
        StructField('principalType',StringType(),True),
        StructField('userType',StringType(),True)
    ]
)

columns = StructType(
    [
        StructField('name',StringType(),True),
        StructField('dataType',StringType(),True),
        StructField('expression',StringType(),True),
        StructField('isHidden',StringType(),True)
    ]
)

measures = StructType(
    [
        StructField('name',StringType(),True),
        StructField('expression',StringType(),True),
        StructField('isHidden',StringType(),True)
    ]
)

source = StructType(
    [
        StructField('expression',StringType(),True)
    ]
)

tables = StructType(
    [
        StructField('name',StringType(),True),
        StructField('columns',ArrayType(columns),True),
        StructField('measures',ArrayType(measures),True),
        StructField('isHidden',StringType(),True),
        StructField('source',ArrayType(source),True),
    ]
)

expressions = StructType(
    [
        StructField('name',StringType(),True),
        StructField('description',StringType(),True),
        StructField('expression',StringType(),True)
    ]
)

datasets = StructType(
    [
        StructField('id',StringType(),True),
        StructField('name',StringType(),True),
        StructField('tables',ArrayType(tables),True),
        StructField('expressions',ArrayType(expressions),True),
        StructField('configuredBy',StringType(),True),
        StructField('configuredById',StringType(),True),
        StructField('isEffectiveIdentityRequired',StringType(),True),
        StructField('isEffectiveIdentityRolesRequired',StringType(),True),
        StructField('directQueryRefreshScheule',StringType(),True),
        StructField('targetStorageMode',StringType(),True),
        StructField('createdDate',StringType(),True),
        StructField('schemaRetrievalError',StringType(),True),
        StructField('contentProviderType',StringType(),True),
        StructField('datasourceUsages',ArrayType(datasourceUsages),True),
        StructField('misconfiguredDatasourcesUsages',ArrayType(datasourceUsages),True),
        StructField('users',ArrayType(datasetUsers),True)
    ]
)

dashboardUsers = StructType(
    [
        StructField('dashboardUserAccessRight',StringType(),True),
        StructField('emailAddress',StringType(),True),
        StructField('displayName',StringType(),True),
        StructField('identifier',StringType(),True),
        StructField('graphId',StringType(),True),
        StructField('principalType',StringType(),True),
        StructField('userType',StringType(),True)
    ]
)

tiles = StructType(
    [
        StructField('id',StringType(),True),
        StructField('reportId',StringType(),True),
        StructField('datasetId',StringType(),True)
    ]
)

dashboards = StructType(
    [
        StructField('id',StringType(),True),
        StructField('displayName',StringType(),True),
        StructField('isReadOnly',StringType(),True),
        StructField('tiles',ArrayType(tiles),True),
        StructField('users',ArrayType(dashboardUsers),True)
    ]
)

reportUsers = StructType(
    [
        StructField('reportUserAccessRight',StringType(),True),
        StructField('emailAddress',StringType(),True),
        StructField('displayName',StringType(),True),
        StructField('identifier',StringType(),True),
        StructField('graphId',StringType(),True),
        StructField('principalType',StringType(),True),
        StructField('userType',StringType(),True)
    ]
)

reports = StructType(
    [
        StructField('reportType',StringType(),True),
        StructField('id',StringType(),True),
        StructField('name',StringType(),True),
        StructField('datasetId',StringType(),True),
        StructField('modifiedDateTime',StringType(),True),
        StructField('originalReportObjectId',StringType(),True),
        StructField('modifiedBy',StringType(),True),
        StructField('modifiedById',StringType(),True),
        StructField('users',ArrayType(reportUsers),True)
    ]
)

workspaces = StructType(
    [
        StructField('id',StringType(),True),
        StructField('name',StringType(),True),
        StructField('description',StringType(),True),
        StructField('type',StringType(),True),
        StructField('state',StringType(),True),
        StructField('isOnDedicatedCapacity',StringType(),True),
        StructField('capacityId',StringType(),True),
        StructField('defaultDatasetStorageFormat',StringType(),True),
        StructField('eventStream',StringType(),True),
        StructField('reports',ArrayType(reports),True),
        StructField('dashboards',ArrayType(dashboards),True),
        StructField('datasets',ArrayType(datasets),True),
        StructField('dataflows',ArrayType(dataflows),True),
        StructField('datamarts',ArrayType(datamarts),True),
        StructField('users',ArrayType(workspaceUsers),True)
    ]
)


# Probably missing possible fields, as they depend on the datasource type
connectionDetails = StructType(
    [
        StructField('url',StringType(),True),
        StructField('sharePointSiteUrl',StringType(),True),
        StructField('path',StringType(),True),
        StructField('server',StringType(),True),
        StructField('database',StringType(),True),
        StructField('extensionDataSourceKind',StringType(),True),
        StructField('extensionDataSourcePath',StringType(),True),
        
    ]
)

instances = StructType(
    [
        StructField('datasourceType',StringType(),True),
        StructField('connectionDetails',connectionDetails,True),
        StructField('datasourceId',StringType(),True),
        StructField('gatewayId',StringType(),True)
    ]
)

misconfigureds = StructType(
    [
        StructField('datasourceType',StringType(),True),
        StructField('connectionDetails',connectionDetails,True),
        StructField('datasourceId',StringType(),True)
    ]
)

#Level 0 Keys

workspacesSchema = StructType(
    [
        StructField('workspaces',ArrayType(workspaces),True),
        StructField('datasourceInstances',ArrayType(instances),True),
        StructField('misconfiguredDatasourceInstances',ArrayType(misconfigureds),True),
        #StructField('ADF_PipelineRunId',StringType(),True),
        #StructField('ADF_PipelineTriggerTime',StringType(),True),
        StructField('processedTimeStamp',StringType(),True),
        StructField('processedDate',StringType(),True)
    ]
)



