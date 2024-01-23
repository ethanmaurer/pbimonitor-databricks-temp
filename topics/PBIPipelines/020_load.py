# Databricks notebook source
# MAGIC %md
# MAGIC # Load Pipelines
# MAGIC This notebook will read a json file from the storage account and append to the transformed.pipelines table.
# MAGIC
# MAGIC It will read the json using the schema from ./011_schema from a path that looks similar to `/apiData/raw/pipelines_getPipelines/2023/06/30/1688164245/pipelines.json`
# MAGIC
# MAGIC Any existing data with the same processedTimeStamp will be deleted, and the new data will be appended.
# MAGIC
# MAGIC `processedTimeStamp` = UNIX timestamp of when the Synapse Pipeline ran
# MAGIC
# MAGIC `processedDate` = The date that the Synapse Pipeline ran

# COMMAND ----------

# MAGIC %md
# MAGIC ### Functions, Schema, and Variables Set-up

# COMMAND ----------

# MAGIC %run ./010_dependencies

# COMMAND ----------

# MAGIC %run ./011_schema

# COMMAND ----------

dbutils.widgets.text("date", "", "")
dbutils.widgets.text("timestamp", "", "")
dbutils.widgets.text("topic", "", "")


processedDate = dbutils.widgets.get("date")
timestamp = dbutils.widgets.get("timestamp")
topic = dbutils.widgets.get("topic")

database = 'transformed'
tableName = topic

tableLocation = f'{database}.{topic}'

readPath = f"/mnt/data-warehouse/apiData/raw/{topic}_getPipelines/{processedDate}/{timestamp}/*.json"
writePath = f"/mnt/data-warehouse/apiData/{database}/{tableName}/delta"  ### REMOVE the /delta don't need to store data in this folder.   ADD a version number to the folder path

# COMMAND ----------

# MAGIC %md
# MAGIC ### JSON Load and Explode

# COMMAND ----------

df = spark.read.json(readPath, pipelinesSchema, multiLine=True)

df.createOrReplaceTempView("tempTable")

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TEMP VIEW exploded
# MAGIC AS (
# MAGIC   select
# MAGIC     json.*
# MAGIC     , processedTimeStamp
# MAGIC     , processedDate
# MAGIC   from tempTable
# MAGIC   LATERAL VIEW explode(value) AS json
# MAGIC )

# COMMAND ----------

# MAGIC %md
# MAGIC ### Delete and Append Data

# COMMAND ----------

deletePartition(database, tableName, 'processedTimeStamp', timestamp)

# COMMAND ----------

appendToDeltaTable(toDf('exploded'), writePath, 'processedTimeStamp')

# COMMAND ----------

sql_optimize_vacuum(database, tableName, writePath)

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP VIEW tempTable;
# MAGIC DROP VIEW exploded;
