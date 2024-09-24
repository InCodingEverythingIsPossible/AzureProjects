# Databricks notebook source
# MAGIC %md
# MAGIC # Access Azure Data Lake using access keys
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
# MAGIC - Set the spark config fs.azure.account.key
# MAGIC - List files from demo container
# MAGIC - Read data from circuits.csv file

# COMMAND ----------

formula1pw_account_key = dbutils.secrets.get(scope = "formula1-scope", key = "formula1pw-account-key")

# COMMAND ----------

spark.conf.set(
    "fs.azure.account.key.formula1pw.dfs.core.windows.net",
    formula1pw_account_key
)

# COMMAND ----------

display(dbutils.fs.ls("abfss://demo@formula1pw.dfs.core.windows.net"))

# COMMAND ----------

display(spark.read.csv("abfss://demo@formula1pw.dfs.core.windows.net/circuits.csv"))

# COMMAND ----------


