# Databricks notebook source
# MAGIC %md
# MAGIC ## Mount the following data lake storage gen2 containers
# MAGIC 1. raw
# MAGIC 2. processed
# MAGIC 3. lookup

# COMMAND ----------

# MAGIC %md
# MAGIC ### Set-up the configs

# COMMAND ----------

configs = {
    "fs.azure.account.auth.type": "CustomAccessToken",
    "fs.azure.account.custom.token.provider.class" : spark.conf.get("spark.databricks.passthrough.adls.gen2.tokenProviderClassName")
}

# COMMAND ----------

# MAGIC %md
# MAGIC ### Mount the raw container

# COMMAND ----------

dbutils.fs.mount(
  source = "abfss://raw@covidreportingpwdl.dfs.core.windows.net/",
  mount_point = "/mnt/covidreportingpwdl/raw",
  extra_configs = configs)

# COMMAND ----------

dbutils.fs.ls("/mnt/covidreportingpwdl/raw")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Mount the processed container

# COMMAND ----------

dbutils.fs.mount(
  source = "abfss://processed@covidreportingpwdl.dfs.core.windows.net/",
  mount_point = "/mnt/covidreportingpwdl/processed",
  extra_configs = configs)

# COMMAND ----------

dbutils.fs.ls("/mnt/covidreportingpwdl/processed")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Mount the lookup container

# COMMAND ----------

dbutils.fs.mount(
  source = "abfss://lookup@covidreportingpwdl.dfs.core.windows.net/",
  mount_point = "/mnt/covidreportingpwdl/lookup",
  extra_configs = configs)

# COMMAND ----------

dbutils.fs.ls("/mnt/covidreportingpwdl/lookup")
