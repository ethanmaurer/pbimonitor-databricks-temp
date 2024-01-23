# Databricks notebook source
# MAGIC %md
# MAGIC #### `readCSVFiles` 
# MAGIC This function will read csv files from the path that is passed to it.  
# MAGIC
# MAGIC params: 
# MAGIC   - path:string  --  the file path that contains the csv files
# MAGIC   - headerIncluded:boolean  --  Interpret the first row as header. 

# COMMAND ----------

def readCSVFiles(path, headerIncluded=True):
  return spark.read.option("header", headerIncluded).csv(path)

# COMMAND ----------

# MAGIC %md 
# MAGIC #### `readCSVFilesWithDelimiter` 
# MAGIC This function will read csv files (or txt) from the path that is passed to it using the provided delimiter as the seperator.
# MAGIC
# MAGIC params: 
# MAGIC   - path:string  --  the file path that contains the csv files
# MAGIC   - delimiter: string -- the delimiter used in the file
# MAGIC   - headerIncluded:boolean  --  Interpret the first row as header. 

# COMMAND ----------

def readCSVFilesWithDelimiter(path, delimiter, headerIncluded=True):
  return spark.read.option("header", headerIncluded).option("delimiter", delimiter).csv(path)

# COMMAND ----------

# MAGIC %md
# MAGIC #### `readExcelFiles` 
# MAGIC This function will read Excel files from the path that is passed to it.  
# MAGIC
# MAGIC params: 
# MAGIC   - path:string  --  the file path that contains the Excel file(s)
# MAGIC   - headerIncluded:boolean  --  Interpret the first row as header 

# COMMAND ----------

def readExcelFiles(path, headerIncluded=True):
  return spark.read.format("com.crealytics.spark.excel").option("header", headerIncluded).csv(path)

# COMMAND ----------

# MAGIC %md
# MAGIC #### `readTxtConvertedCsvFiles`
# MAGIC This function reads a CSV file from Toogood which was converted from a TXT file (extract)
# MAGIC The HEADER and TRAILER rows will be removed from the raw file, and a predefined set of headers will be used
# MAGIC
# MAGIC params:
# MAGIC   - path:string -- the file path that contains the csv files
# MAGIC   - headers:[string] -- predefined list of headers
# MAGIC   - remove_top_rows:int -- determine the number of rows to remove from the top
# MAGIC   - remove_bottom_rows:int -- determine the number of rows to remove from the bottom

# COMMAND ----------

from pyspark.sql.functions import col
from pyspark.sql.types import *


def readTxtConvertedCsvFiles(path, headers=[], remove_top_rows=0, remove_bottom_rows=0, desiredDelimiter=","):
  user_schema = StructType(
    [
      StructField(x, StringType(), True)
      for x in headers
    ]
  )
  # we define the schema here because of uneven columns
  df = spark.read.schema(user_schema).options(header='false', delimiter=desiredDelimiter, multiLine=True).csv(path)

  tmp = df.collect()[remove_top_rows:df.count() - remove_bottom_rows]
  return spark.createDataFrame(tmp, user_schema) 


# COMMAND ----------

# MAGIC %md
# MAGIC #### readDeltaTable

# COMMAND ----------

def readDeltaTable(path):
  return spark.read.format("delta").load(path)

# COMMAND ----------

# MAGIC %md
# MAGIC #### `readJsonFiles` 
# MAGIC This function will read json files from the path that is passed to it.  
# MAGIC
# MAGIC params: 
# MAGIC   - path:string  --  the file path that contains the json files
# MAGIC   - jsonSchema:Struct  --  

# COMMAND ----------

def readJsonFiles(path, jsonSchema):
  return spark.read.schema(jsonSchema).json(path, multiLine=True)
  

