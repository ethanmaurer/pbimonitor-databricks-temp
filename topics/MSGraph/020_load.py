# Databricks notebook source
# MAGIC %md
# MAGIC # Load MSGraph
# MAGIC This notebook will read a json file from the storage account and append to the transformed.securityGroups table, for example.
# MAGIC
# MAGIC It will read the json using the schema from ./011_schema from a path that looks similar to `/apiData/raw/MSGraph_getSecurityGroups/2023/06/30/1688164245/security-groups.json`
# MAGIC
# MAGIC Any existing data with the same processedTimeStamp will be deleted, and the new data will be appended.
# MAGIC
# MAGIC `processedTimeStamp` = UNIX timestamp of when the Pipeline ran
# MAGIC
# MAGIC `processedDate` = The date that the pipeline ran

# COMMAND ----------

# MAGIC %md
# MAGIC ### Functions, Schema, and Variables Set-up

# COMMAND ----------

# MAGIC %run ./010_dependencies

# COMMAND ----------

# MAGIC %run ./011_schemas

# COMMAND ----------

dbutils.widgets.text("date", "", "")
dbutils.widgets.text("timestamp", "", "")
dbutils.widgets.text("topic", "", "")


processedDate = dbutils.widgets.get("date")
timestamp = dbutils.widgets.get("timestamp")
topic = dbutils.widgets.get("topic")

database = "transformed"

tables = ["securityGroups", "subscribedSKUs", "users", "usersInSecurityGroups"]

# Build read paths:
readPaths = {}
for table in tables:
    capTable = table[0].upper() + table[1:]
    readPaths[
        table
    ] = f"/mnt/data-warehouse/apiData/raw/{topic}_get{capTable}/{processedDate}/{timestamp}/*.json"

writePaths = {}
for table in tables:
    writePaths[table] = f"/mnt/data-warehouse/apiData/{database}/{topic}_{table}/delta"   ### REMOVE the /delta don't need to store data in this folder.   ADD a version number to the folder path

# COMMAND ----------

# MAGIC %md
# MAGIC ### JSON Load and Explode

# COMMAND ----------

import time

dfs = {}

for table in tables:
    start = time.time()
    dfs[table] = spark.read.json(readPaths[table], schemas[table], multiLine=True)
    dfs[table].createOrReplaceTempView(table)
    end = time.time()
    dif = round(end - start, 2)
    print(f'{table} took {dif} seconds')

# COMMAND ----------

for table in tables:
    start = time.time()
    spark.sql(f'''
    CREATE OR REPLACE TEMP VIEW exploded{table}
    AS (
        select * EXCEPT(json, value) 
        , json.id AS {table[:-1]}Id
        , json.* EXCEPT(id)
        FROM {table}
        LATERAL VIEW explode(value) AS json
    )
    ''')
    end = time.time()
    dif = round(end - start, 2)
    print(f'{table} took {dif} seconds')

# COMMAND ----------

# MAGIC %md
# MAGIC ### Delete and Append Data

# COMMAND ----------

for table in tables:
    start = time.time()
    deletePartition(database, f'{topic}_{table}', 'processedTimeStamp', timestamp)
    end = time.time()
    dif = round(end - start, 2)
    print(f'{table} took {dif} seconds')

# COMMAND ----------

for table in tables:
    start = time.time()
    appendToDeltaTable(toDf(f'exploded{table}'), writePaths[table], 'processedTimeStamp')
    end = time.time()
    dif = round(end - start, 2)
    print(f'{table} took {dif} seconds')

# COMMAND ----------

for table in tables:
    start = time.time()
    sql_optimize_vacuum(database, f'{topic}_{table}', writePaths[table])
    end = time.time()
    dif = round(end - start, 2)
    print(f'{table} took {dif} seconds')

# COMMAND ----------

for table in tables:
    spark.sql(f'DROP VIEW {table}')
    spark.sql(f'DROP VIEW exploded{table}')

