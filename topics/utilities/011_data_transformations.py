# Databricks notebook source
def getReadPath(database, tableName, date):
    return f'/mnt/data-warehouse/{database}/{tableName}/raw/{date}'


# COMMAND ----------

def getWritePath(database, tableName, version ):
    finalTable = f'{tableName}_v{version}'
    return f'/mnt/data-warehouse/{database}/{tableName}/delta/{finalTable}'

# COMMAND ----------

# MAGIC %md 
# MAGIC #### `replaceSpecialCharsInColumns`
# MAGIC Replaces the spaces in column names with `_` for the given dataframe.  It will also replace paranthesis with brackets.  For example given a dataframe with the column names, "Account Name", "Account Code", "Account(Key)" will become a new dataframe with the columns: "Account_Name", "Account_Code", "Account[Key]"
# MAGIC
# MAGIC When it comes to writing dataframes, spaces can cause issues.
# MAGIC
# MAGIC params:
# MAGIC  - df: Data frame with columns to rename

# COMMAND ----------

def replaceSpecialCharsInColumns(df):
  
  newDf = df
  
  for col in df.columns:
    # iterate through each column and replace spaces with _
    newDf = newDf.withColumnRenamed(col,col.replace(" ", "").replace("(","[").replace(")","]").replace(".","_").replace(":","_").replace("[","_").replace("]","").replace("@","").replace("\n", "").replace("\t", ""))
    
  
  return newDf

# COMMAND ----------

# MAGIC %md
# MAGIC #### `sql_create_table`
# MAGIC Creates a SQL table if it does not exist for the specified table name
# MAGIC
# MAGIC params:
# MAGIC - database: name of database. IE Bronze, Silver, Gold
# MAGIC - tableName: The name of the created SQL table name that lives in the database
# MAGIC - writePath: The physical location where the data is stored in the Data Lake

# COMMAND ----------

def sql_create_table(database, tableName, writePath):
   
  # Check to make sure that there is a defined SQL table
  # if table is not created, create a table
  spark.sql(f"""
    create table if not exists {database}.{tableName}
    using DELTA
    location '{writePath}' 
    """)

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC #### `sql_optimize_vacuum`
# MAGIC
# MAGIC Runs three processes
# MAGIC 1. Creates a SQL table if it does not exist for the specified table name
# MAGIC 1. Runs an optimize step to reduce the number of files in the Delta Lake [Docs on Optimize](https://docs.microsoft.com/en-us/azure/databricks/delta/optimizations/file-mgmt#delta-optimize)
# MAGIC 1. Vacuum Removes older files that are no longer used within the Delta Table for table versioning [Docs on Vacuum](https://docs.microsoft.com/en-us/azure/databricks/spark/latest/spark-sql/language-manual/delta-vacuum)
# MAGIC
# MAGIC params:
# MAGIC - database: name of database. IE Bronze, Silver, Gold
# MAGIC - tableName: The name of the created SQL table name that lives in the database
# MAGIC - writePath: The physical location where the data is stored in the Data Lake

# COMMAND ----------

def sql_optimize_vacuum(database, tableName, writePath):
   
  # Check to make sure that there is a defined SQL table
  # if table is not created, create a table
  spark.sql(f"""
    create table if not exists {database}.{tableName}
    using DELTA
    location '{writePath}' 
    """)
  
  #  Optimize the Delta Table
  output = spark.sql(f"""
    OPTIMIZE {database}.{tableName}
    """)
  
  # Run the Vacuum step to remove un-needed delta table files
  spark.sql(f"""
    VACUUM {database}.{tableName}
    """)
  
  return output

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC #### `deletePartition`
# MAGIC
# MAGIC Checks to see if the delta table exisits in the database. If yes, then deletes a single partition based on the referenced partitionKey and value.
# MAGIC
# MAGIC params:
# MAGIC - database: name of database. IE Bronze, Silver, Gold
# MAGIC - tableName: The name of the created SQL table name that lives in the database
# MAGIC - partitionKey: The Partition Key that is used in the delta table. Currenly only supports a single key (column) from the delta table
# MAGIC - value: The value of the partition name that is for deletion

# COMMAND ----------

def deletePartition(database, tableName, partitionKey, value):

  if spark._jsparkSession.catalog().tableExists(database, tableName):
    spark.sql(f"""
      DELETE FROM {database}.{tableName} WHERE {partitionKey} = '{value}'
      """)
    return print(f"Deleted data for {database}.{tableName} on partition key of {partitionKey} for value of {value}")

  else:
    return print("Skipped delete step. table does not exist")

# COMMAND ----------



# COMMAND ----------

# MAGIC %md
# MAGIC #### `toDf`
# MAGIC Returns an sql temporary view as a pyspark.sql dataframe
# MAGIC
# MAGIC params:
# MAGIC - tempName: Name of the temporary view to be converted

# COMMAND ----------

def toDf(tempName):
    df = spark.sql(f'SELECT * FROM {tempName}')
    return df
