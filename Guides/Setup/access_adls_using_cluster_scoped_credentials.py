# Databricks notebook source
# MAGIC %md
# MAGIC # Access Azure Data Lake using cluster scoped credentials
# MAGIC
# MAGIC ## List of tasks to do
# MAGIC - Copy access key from storage account (left of the list Security & Networking section)
# MAGIC - Create Azure Key Vault (if it is not created for the project)
# MAGIC     1. Create Azure Key Vault Service
# MAGIC     2. Go to Secrets Session (left of the list Objects section)
# MAGIC     3. Click Generate/Import
# MAGIC     4. Enter a values and create secret
# MAGIC - Connect Secret scope on Databricks with Azure Key Vault (if it is not connected to Azure Key Vault)
# MAGIC     1. Go to the main page of Databricks
# MAGIC     2. In the url at the end add -> #secrets/createScope
# MAGIC     3. Copy from Azure Key Vault properties DNS Name (Vault URI) and Resource ID
# MAGIC - Set the spark config fs.azure.account.key in the cluster
# MAGIC   1. Go to compute section in Databricks and choose cluster which you want to edit
# MAGIC   2. Then go to the advanced options and fulfill information in spark config section in format like below (name value)
# MAGIC   3. fs.azure.account.key.formula1pw.dfs.core.windows.net {{secrets/secret_scope_name/secret_name}}
# MAGIC   4. formula1pw in name is storage account name
# MAGIC   5. Secret scope name is name scope in databricks which we entered
# MAGIC   6. Secret name is name of secret which we have in Azure Key Vault which we created
# MAGIC - List files from demo container
# MAGIC - Read data from circuits.csv file

# COMMAND ----------

display(dbutils.fs.ls("abfss://demo@formula1pw.dfs.core.windows.net"))

# COMMAND ----------

display(spark.read.csv("abfss://demo@formula1pw.dfs.core.windows.net/circuits.csv"))
