# Databricks notebook source
from pyspark.sql.types import *

group = [
    StructField('id',StringType(),True),
    StructField('name',StringType(),True),
]

capacity = [
    StructField('id',StringType(),True),
    StructField('displayName',StringType(),True),
    StructField('sku',StringType(),True),
]

refreshSchedule = [
    StructField('days',ArrayType(StringType()),True),
    StructField('times',ArrayType(StringType()),True),
    StructField('enabled',StringType(),True),
    StructField('localTimeZoneId',StringType(),True),
    StructField('notifyOption',StringType(),True)
]

lastRefresh = [
    StructField('id',StringType(),True),
    StructField('refreshType',StringType(),True),
    StructField('startTime',StringType(),True),
    StructField('endTime',StringType(),True),
    StructField('serviceExceptionJson',StringType(),True),
    StructField('status',StringType(),True),
    StructField('requestId',StringType(),True),
    StructField('extendedStatus',StringType(),True),
]

refreshables = StructType(
    [
        StructField('id',StringType(),True),
        StructField('name',StringType(),True),
        StructField('kind',StringType(),True),
        StructField('lastRefresh',StructType(lastRefresh),True),
        StructField('refreshSchedule',StructType(refreshSchedule),True),
        StructField('configuredBy',ArrayType(StringType()),True),
        StructField('capacity',StructType(capacity),True),
        StructField('group',StructType(group),True)
    ]
)

refreshablesSchema = StructType(
  [
    #StructField('@odata.context',StringType(),True),
    #StructField('@odata.count',StringType(),True),
    StructField('value',ArrayType(refreshables),True),
    StructField('processedDate',StringType(),True),
    StructField('processedTimeStamp',StringType(),True)
  ]
)

# COMMAND ----------


