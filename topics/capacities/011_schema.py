# Databricks notebook source
# MAGIC %md
# MAGIC #Capacities Schema

# COMMAND ----------

from pyspark.sql.types import *

capacities = StructType(
        [
            StructField('id',StringType(),True),
            StructField('displayName',StringType(),True),
            StructField('admins',ArrayType(StringType()),True),
            StructField('sku',StringType(),True),
            StructField('state',StringType(),True),
            StructField('capacityUserAccessRight',StringType(),True),
            StructField('region',StringType(),True),
            StructField('users',ArrayType(StringType()),True)
        ]
    )

capacitiesSchema = StructType(
    [
        StructField('value',ArrayType(capacities),True),
        StructField('processedTimeStamp',StringType(),True),
        StructField('processedDate',StringType(),True)
    ]
)

capacityUsers = StructType(
    [
        StructField('capacityUserAccessRight',StringType(),True),
        StructField('emailAddress',StringType(),True),
        StructField('displayName',StringType(),True),
        StructField('identifier',StringType(),True),
        StructField('graphId',StringType(),True),
        StructField('principalType',StringType(),True),
        StructField('profile',StringType(),True)
    ]
)

capacityUsersSchema = StructType(
    [
        StructField('@odata.context',StringType(),True),
        StructField('value',ArrayType(capacityUsers),True),
        StructField('processedDate',StringType(),True),
        StructField('processedTimeStamp',StringType(),True),
        StructField('capacityId',StringType(),True)
    ]
)
