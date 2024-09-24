# Databricks notebook source
# MAGIC %md
# MAGIC #Access External datalake

# COMMAND ----------

dbutils.fs.ls('abfss://demo@databrickscourse.dfs.core.windows.net/')
