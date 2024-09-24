# Databricks notebook source
# MAGIC %md
# MAGIC # Mount Azure Data Lake using credential passthrough
# MAGIC
# MAGIC ## List of tasks to do
# MAGIC - Click checkbox Azure Data Lake Storage credential passthrough in advanced options of cluster configuration
# MAGIC   1. Compute section in Databricks
# MAGIC   2. Click cluster which you want to update
# MAGIC   3. Click checkbox to allow credential passthrough
# MAGIC - Assign Role 'Storage Blob Data Contributor' to the Azure Data Lake in IAM of Storage Account for the user
# MAGIC   1. Go to the Access Control (IAM) of Storage Account
# MAGIC   2. Click Add -> choose Add role assignment
# MAGIC   3. Find 'Storage Blob Data Contributor' from the list and click next
# MAGIC   4. Grant access to user in select member option
# MAGIC - Set the spark configuration using credential passthrough
# MAGIC - List files from demo container

# COMMAND ----------

configs = {
    "fs.azure.account.auth.type": "CustomAccessToken",
    "fs.azure.account.custom.token.provider.class" : spark.conf.get("spark.databricks.passthrough.adls.gen2.tokenProviderClassName")
}

dbutils.fs.mount(
    source = "abfss://demo@formula1pw.dfs.core.windows.net/",
    mount_point = "/mnt/formula1pw/demo",
    extra_configs = configs
)

# COMMAND ----------

display(dbutils.fs.ls("/mnt/formula1pw/demo"))
