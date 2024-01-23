# Databricks notebook source
# MAGIC %md
# MAGIC # Pipelines Schema

# COMMAND ----------

from pyspark.sql.types import *

stages = StructType(
    [
        StructField('order',StringType(),True),
        StructField('workspaceId',StringType(),True),
        StructField('workspaceName',StringType(),True)
    ]
)

pipelines = StructType(
    [
        StructField('id',StringType(),True),
        StructField('displayName',StringType(),True),
        StructField('description',StringType(),True),
        StructField('stages',ArrayType(stages),True)
    ]
)

pipelinesSchema = StructType(
    [
        #StructField('@odata.context',StringType(),True),
        StructField('value',ArrayType(pipelines),True),
        StructField('processedTimeStamp',StringType(),True),
        StructField('processedDate',StringType(),True)
    ]
)

pipelineUsers = StructType(
    [
        StructField('accessRight',StringType(),True),
        StructField('identifier',StringType(),True),
        StructField('principalType',StringType(),True)
    ]
)

pipelineUsersSchema = StructType(
    [
        #StructField('@odata.context',StringType(),True),
        StructField('value',ArrayType(pipelineUsers),True),
        StructField('processedTimeStamp',StringType(),True),
        StructField('processedDate',StringType(),True),
        StructField('pipelineId',StringType(),True)
    ]
)


# COMMAND ----------


