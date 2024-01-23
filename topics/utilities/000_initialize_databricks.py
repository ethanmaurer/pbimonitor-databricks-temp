# Databricks notebook source
def mountFolder(dstStorageAccount, dstContainerName, dstFolderPath):
# setup variables
  containerName     = dstContainerName
  folderName        = dstFolderPath
  storageAccount    = dstStorageAccount
  appRegistrationId = dbutils.secrets.get(scope = "azure-key-vault", key = "powerbi-app-id")
  directoryId       = dbutils.secrets.get(scope = "azure-key-vault", key = "tenant-id")
  
  # Get the key vault secret for databricks
  clientSecret = dbutils.secrets.get(scope = "azure-key-vault", key = "powerbi-client-secret")
  
  storageSource = f"abfss://{containerName}@{storageAccount}.dfs.core.windows.net/{folderName}"
  fullMountLocationName = f"/mnt/{containerName}/{folderName}"
  
  configs = {
    "fs.azure.account.auth.type"                : "OAuth"
    , "fs.azure.account.oauth.provider.type"    : "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider"
    , "fs.azure.account.oauth2.client.id"       : appRegistrationId
    , "fs.azure.account.oauth2.client.secret"   : clientSecret
    , "fs.azure.account.oauth2.client.endpoint" : f"https://login.microsoftonline.com/{directoryId}/oauth2/token"
  }
  
  try:
    dbutils.fs.mount(
        source = storageSource,
        mount_point = fullMountLocationName,
        extra_configs = configs
    )
    print(f"Mount Location Created at: /mnt/{containerName}/{folderName}")
    
  except Exception as e:
    print('Error occurred while mounting', str(e))


# COMMAND ----------

mountFolder("powerbimonitor02","data-warehouse2","apiData2")

# COMMAND ----------

display(dbutils.fs.mounts())

# COMMAND ----------

dbutils.fs.unmount("/mnt/data-warehouse2/apiData2")

# COMMAND ----------

# MAGIC %sql
# MAGIC create database if not exists transformed

# COMMAND ----------

# MAGIC %sql
# MAGIC create database if not exists currated

# COMMAND ----------

dbutils.fs.help()

# COMMAND ----------

dbutils.fs.rm('dbfs:/mnt/data-warehouse/transformed', True)
