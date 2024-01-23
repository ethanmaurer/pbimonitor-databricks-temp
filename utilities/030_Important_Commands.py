# Databricks notebook source
# MAGIC %md 
# MAGIC
# MAGIC # Removing a widget
# MAGIC
# MAGIC Use this command to remove a widget from a notebook

# COMMAND ----------

dbutils.widgets.remove("widget_name")
