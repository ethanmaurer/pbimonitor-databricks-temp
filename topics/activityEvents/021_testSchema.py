# Databricks notebook source
# MAGIC %md
# MAGIC #Test Activity Events Schema
# MAGIC This notebooks compares json data loaded from the activityEventsSchema and data loaded using an inferedSchema and returns the columns that are missing from the schema to the original notebook

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

readPath = f"/mnt/data-warehouse/apiData/raw/{topic}_getActivityEvents/{processedDate}/{timestamp}/*.json"

# COMMAND ----------

# MAGIC %md
# MAGIC ### JSON Load and Explode

# COMMAND ----------

inferDf = spark.read.json(readPath, multiLine=True)
inferDf.createOrReplaceTempView('infer')

# COMMAND ----------

schemaDf = spark.read.json(readPath, activityEventSchema, multiLine=True)
schemaDf.createOrReplaceTempView('schema')

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TEMP VIEW explodedInfer
# MAGIC AS (
# MAGIC   SELECT json.*
# MAGIC   , activityDate
# MAGIC   , processedTimeStamp
# MAGIC   , processedDate
# MAGIC   FROM infer
# MAGIC   LATERAL VIEW explode(activityEventEntities) AS json
# MAGIC )

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TEMP VIEW explodedSchema
# MAGIC AS (
# MAGIC   SELECT json.*
# MAGIC   , activityDate
# MAGIC   , processedTimeStamp
# MAGIC   , processedDate
# MAGIC   FROM schema
# MAGIC   LATERAL VIEW explode(activityEventEntities) AS json
# MAGIC )

# COMMAND ----------

# MAGIC %md
# MAGIC ###Compare Columns

# COMMAND ----------

inferColumns = toDf('explodedInfer').columns
schemaColumns = toDf('explodedSchema').columns

# COMMAND ----------

missing_cols = []
for col in inferColumns:
    if not col in schemaColumns:
        missing_cols.append(col)
        

# COMMAND ----------

# MAGIC %md
# MAGIC ###Drop Views and Return

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP VIEW explodedInfer;
# MAGIC DROP VIEW explodedSchema;

# COMMAND ----------

if len(missing_cols) == 0:
    dbutils.notebook.exit('False')
else:
    dbutils.notebook.exit(missing_cols)
