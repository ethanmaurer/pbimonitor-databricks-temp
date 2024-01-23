# Databricks notebook source
# MAGIC %run ./010_import_all

# COMMAND ----------

df = spark.sql(" select * from hive_metastore.transformed.activityevents_fact ")

# COMMAND ----------

display(df)

# COMMAND ----------

df.write.format("delta").mode("append").option("mergeSchema", True).save("/mnt/data-warehouse2/apiData2/activityevents_fact_v1")

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from transformed.activityevents_fact
