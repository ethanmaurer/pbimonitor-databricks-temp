# Databricks notebook source
# MAGIC %md
# MAGIC # Tenant Settings Schema

# COMMAND ----------

from pyspark.sql.types import *

enabledSecurityGroups = StructType(
    [
        StructField('graphId',StringType(),True),
        StructField('name',StringType(),True)
    ]
)

tenantSettings = StructType(
    [
        StructField('settingName',StringType(),True),
        StructField('title',StringType(),True),
        StructField('enabled',StringType(),True),
        StructField('canSpecifySecurityGroups',StringType(),True),
        StructField('enabledSecurityGroups',ArrayType(enabledSecurityGroups),True),
        StructField('tenantSettingGroup',StringType(),True)
    ]
)

settingsSchema = StructType(
    [
        StructField('tenantSettings',ArrayType(tenantSettings),True),
        StructField('processedTimeStamp',StringType(),True),
        StructField('processedDate',StringType(),True)
    ]
)
