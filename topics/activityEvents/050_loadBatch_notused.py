# Databricks notebook source
# MAGIC %md
# MAGIC # Load Activity Events in Batch
# MAGIC This notebook will read json files from the storage account and append to the transformed.activityEvents table.
# MAGIC
# MAGIC It will read the json using the schema from ./011_schema from a path that looks similar to `/apiData/raw/activityEvents_getActivityEvents/2023/06/30/1688164245/powerBI-activity.json`
# MAGIC
# MAGIC The new data will overwrite any existing data from the same activity date
# MAGIC
# MAGIC `processedTimeStamp` = UNIX timestamp of when the Pipeline ran
# MAGIC
# MAGIC `activityDate` = The date that the getActivityEvents API got data for. Yesterday, by default
# MAGIC
# MAGIC `processedDate` = The date that the pipeline ran
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
dbutils.widgets.text("dayCount", "", "")

processedDate = dbutils.widgets.get("date")
timestamp = dbutils.widgets.get("timestamp")
topic = dbutils.widgets.get("topic")
dayCount = int(dbutils.widgets.get("dayCount"))

database = 'transformed'
tableName = 'activityEvents'

tableLocation = f'{database}.{topic}'

readPath = f"/mnt/data-warehouse/apiData/raw/{topic}_getActivityEvents/{processedDate}/{timestamp}/*.json"
writePath = f"/mnt/data-warehouse/apiData/{database}/{tableName}/delta"

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

missingCols = dbutils.notebook.run('./040_testSchema', 60, {"date": processedDate, "timestamp": timestamp, "topic": topic})

# COMMAND ----------

from pyspark.sql.functions import *

finalDf = toDf('exploded').withColumn('missingCols', lit(str(missingCols)))

# COMMAND ----------

# MAGIC %md
# MAGIC ###Build list of dates

# COMMAND ----------

import datetime
today = datetime.datetime.today()
date_list = [today - datetime.timedelta(days=x+1) for x in range(dayCount)]
print(date_list)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Delete and Append

# COMMAND ----------

for date in date_list:
    activityDate = date.strftime('%Y-%m-%d') 
    deletePartition(database, tableName, 'activityDate', activityDate)

# COMMAND ----------

appendToDeltaTable(finalDf, writePath, 'activityDate')

# COMMAND ----------

sql_optimize_vacuum('transformed', 'activityEvents', writePath)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Drop Temp Views

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP VIEW tempTable;
# MAGIC DROP VIEW exploded;
