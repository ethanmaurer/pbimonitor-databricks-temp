# Databricks notebook source
# MAGIC %md
# MAGIC # Load Scan Result
# MAGIC This notebook will read a json file from the storage account and overwrite the transformed.scan table.
# MAGIC
# MAGIC It will read the json using the schema from ./011_schema from a path that looks similar to `/apiData/raw/scan_result_getScanResults/2023/06/30/1688164245/scan-result.json`
# MAGIC
# MAGIC The new data will overwrite the existing data.
# MAGIC
# MAGIC `processedTimeStamp` = UNIX timestamp of when the Pipeline ran
# MAGIC
# MAGIC `processedDate` = The date that the pipeline ran

# COMMAND ----------

# MAGIC %md
# MAGIC ## Functions, Schema, and Variables Set-up

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

readPath = f"/mnt/data-warehouse/apiData/raw/{topic}_getScanResults/{processedDate}/{timestamp}/*.json"
writePath = f"/mnt/data-warehouse/apiData/{database}/{tableName}/delta"  ### REMOVE the /delta don't need to store data in this folder.   ADD a version number to the folder path

# COMMAND ----------

# MAGIC %md
# MAGIC ## JSON Load and Explode

# COMMAND ----------

df = spark.read.json(readPath, workspacesSchema, multiLine=True)

df.createOrReplaceTempView("tempTable")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Overwrite all data

# COMMAND ----------

overwritePartitionDeltaTable(df, writePath, 'processedTimeStamp')

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC Commented out deletePartition & appendToDeltaTable because we only want to load the current records of all catalog items. 
# MAGIC
# MAGIC If you want to track items over time these steps would be added back in.

# COMMAND ----------

#deletePartition(database, tableName, 'processedTimeStamp', timestamp)

# COMMAND ----------

#appendToDeltaTable(df, writePath, 'processedTimeStamp')

# COMMAND ----------

sql_optimize_vacuum(database, tableName, writePath)

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP VIEW tempTable;
