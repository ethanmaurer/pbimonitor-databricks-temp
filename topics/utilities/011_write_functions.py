# Databricks notebook source
# MAGIC %md
# MAGIC #### `appendToDeltaTable`
# MAGIC Create a deltaTable from the given dataframe, and save it to the desired location.  
# MAGIC **Note**: This function assumes the processed date is today.
# MAGIC 
# MAGIC params:
# MAGIC  - df: Dataframe to save as delta table.
# MAGIC  - path: Location to save the delta table.
# MAGIC  - partition: defines a partition schema for writing files into the delta table [Microsoft Docs on Delta Tables](https://docs.microsoft.com/en-us/azure/databricks/delta/best-practices)

# COMMAND ----------

def appendToDeltaTable(df, path, partition):
  df.write.format("delta").mode("append").option("mergeSchema", True).partitionBy(partition).save(path)

# COMMAND ----------

# MAGIC %md
# MAGIC #### `overwritePartitionDeltaTable`
# MAGIC Create a deltaTable from the given dataframe, and save it to the desired location. It will overwrite the delta table if it exists already.  This will also partition the table by the partition key provided.
# MAGIC 
# MAGIC params:
# MAGIC  - df: Dataframe to save as delta table.
# MAGIC  - path: Location to save the delta table.
# MAGIC  - partition: defines a partition schema for writing files into the delta table [Microsoft Docs on Delta Tables](https://docs.microsoft.com/en-us/azure/databricks/delta/best-practices)

# COMMAND ----------

# Overwrite delta table
def overwritePartitionDeltaTable(df, path, partition):
  df.write.format("delta").mode("overwrite").option("overwriteSchema", True).partitionBy(partition).save(path)

# COMMAND ----------

# MAGIC %md
# MAGIC #### `overwriteDeltaTable`
# MAGIC Create a deltaTable from the given dataframe, and save it to the desired location. It will overwrite the delta table if it exists already. 
# MAGIC 
# MAGIC params:
# MAGIC  - df: Dataframe to save as delta table.
# MAGIC  - path: Location to save the delta table.
# MAGIC  

# COMMAND ----------

# Overwrite delta table
def overwriteDeltaTable(df, path):
  df.write.format("delta").mode("overwrite").option("overwriteSchema", True).save(path)

# COMMAND ----------

# MAGIC %md
# MAGIC #### `logDataForProcessedDate`
# MAGIC This function will create a table to log how many rows of data were written into the table for the processed date.
# MAGIC 
# MAGIC params:
# MAGIC  - database: Database that contains the table that is desired to log data about: eg. bronze, silver, gold
# MAGIC  - tableName: name of the table to log about
# MAGIC  - processedDate: date to log about
# MAGIC  - (optional) path: location to write log table to, defaulted to **"/mnt/data-warehouse/utilities/logging_v1"**
# MAGIC  

# COMMAND ----------

def logDataForProcessedDate(database, tableName, processedDate, path = "/mnt/data-warehouse/utilities/logging_v1"):
  
  df = spark.sql(f"""
    
    select 
      date_format(current_timestamp(), 'yyyy-MM-dd hh:mm:ss' ) as DateTimeUTC
      , '{database}.{tableName}' as Source
      , count(*) as RowCount
    from {database}.{tableName} where ProcessedDate = '{processedDate}'
    group by 1, 2
    
    """)
  
  #append to data-quality table
  df.write.format("delta").mode("append").save(path)

# COMMAND ----------

def logDataForInputFileByProcessedDate(database, tableName, processedDate, path = "/mnt/data-warehouse/utilities/logging_raw_files_v1"):
  
  df = spark.sql(f"""
    
    select 
      date_format(current_timestamp(), 'yyyy-MM-dd hh:mm:ss' ) as DateTimeUTC
      , '{database}.{tableName}' as Source
      , rawFile
      , count(*) as RowCount
    from {database}.{tableName} where ProcessedDate = '{processedDate}'
    group by 1, 2, 3

    """)
  
  #append to data-quality table
  df.write.format("delta").mode("append").save(path)

# COMMAND ----------


