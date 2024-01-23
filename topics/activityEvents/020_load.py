# Databricks notebook source
# MAGIC %md
# MAGIC # Load Activity Events
# MAGIC This notebook will read a json file from the storage account and append to the transformed.activityEvents table.
# MAGIC
# MAGIC It will read the json using the schema from ./011_schema from a path that looks similar to `/apiData/raw/activityEvents_getActivityEvents/2023/06/30/1688164245/powerBI-activity.json`
# MAGIC
# MAGIC The new data will overwrite any existing data from the same activity date
# MAGIC
# MAGIC `processedTimeStamp` = UNIX timestamp of when the Pipeline ran
# MAGIC
# MAGIC `processedDate` = The date that the pipeline ran
# MAGIC
# MAGIC `activityDate` = The date that the getActivityEvents API got data for. Yesterday, by default
# MAGIC
# MAGIC
# MAGIC `missingCols` = Columns missing from the schema according to the ./051_testSchema notebook

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
dbutils.widgets.text("activityDate", "", "")


processedDate = dbutils.widgets.get("date")
timestamp = dbutils.widgets.get("timestamp")
topic = dbutils.widgets.get("topic")
activityDate = dbutils.widgets.get("activityDate")

database = 'transformed'
tableName = topic

readPath = f"/mnt/data-warehouse/apiData/raw/{topic}_getActivityEvents/{processedDate}/{timestamp}/*.json"
writePath = f"/mnt/data-warehouse/apiData/{database}/{tableName}/delta" ### REMOVE the /delta don't need to store data in this folder.   ADD a version number to the folder path

# COMMAND ----------

# MAGIC %md
# MAGIC ### JSON Load and Explode

# COMMAND ----------

df = spark.read.json(readPath, activityEventSchema, multiLine=True)

df.createOrReplaceTempView("tempTable")

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TEMP VIEW exploded
# MAGIC AS (
# MAGIC   SELECT json.*
# MAGIC   , activityDate
# MAGIC   , processedTimeStamp
# MAGIC   , processedDate
# MAGIC   FROM tempTable
# MAGIC   LATERAL VIEW explode(activityEventEntities) AS json
# MAGIC )

# COMMAND ----------

# MAGIC %md
# MAGIC ### Add missingCols column

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC This loads changes into a single column called 'missingCols' it indicates to us that the schema has added a new column. We can take action from this to look the date where this new column appeared and re-investigate the new column
# MAGIC
# MAGIC Could have this column inform our report
# MAGIC
# MAGIC Could have this alert users (**admins**) of changes in schema for missing data

# COMMAND ----------

missingCols = dbutils.notebook.run('./021_testSchema', 60, {"date": processedDate, "timestamp": timestamp, "topic": topic})

# COMMAND ----------

from pyspark.sql.functions import *

finalDf = toDf('exploded').withColumn('missingCols', lit(str(missingCols)))

# COMMAND ----------

# MAGIC %md
# MAGIC ### Delete and Append

# COMMAND ----------

deletePartition(database, tableName, 'activityDate', activityDate)

# COMMAND ----------

appendToDeltaTable(finalDf, writePath, 'activityDate')

# COMMAND ----------

sql_optimize_vacuum(database, tableName, writePath)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Drop Temp Views

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP VIEW tempTable;
# MAGIC DROP VIEW exploded;
