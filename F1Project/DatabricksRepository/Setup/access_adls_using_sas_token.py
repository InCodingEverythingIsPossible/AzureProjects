# Databricks notebook source
# MAGIC %md
# MAGIC # Access Azure Data Lake using SAS Token
# MAGIC
# MAGIC ## List of tasks to do
# MAGIC - Generate SAS token of container level in storage account on which we want to work
# MAGIC   1. Enter to the storage account
# MAGIC   2. Choose container in which you want to generate SAS Token
# MAGIC   3. Click 3 dots or right click on container which you want use
# MAGIC   4. Set correct permissions to SAS Token
# MAGIC   5. Click Generate SAS
# MAGIC - Create Azure Key Vault (if it is not created for the project)
# MAGIC     1. Create Azure Key Vault Service
# MAGIC     2. Go to Secrets Session (left of the list Objects section)
# MAGIC     3. Click Generate/Import
# MAGIC     4. Enter a values and create secret
# MAGIC - Connect Secret scope on Databricks with Azure Key Vault (if it is not connected to Azure Key Vault)
# MAGIC     1. Go to the main page of Databricks
# MAGIC     2. In the url at the end add -> #secrets/createScope
# MAGIC     3. Copy from Azure Key Vault properties DNS Name (Vault URI) and Resource ID
# MAGIC - Set the spark config for SAS Token
# MAGIC - List files from demo container
# MAGIC - Read data from circuits.csv file

# COMMAND ----------

formula1_sas_token = dbutils.secrets.get(scope = "formula1-scope", key = "formula1pw-sas-token")

# COMMAND ----------

spark.conf.set("fs.azure.account.auth.type.formula1pw.dfs.core.windows.net", "SAS")
spark.conf.set("fs.azure.sas.token.provider.type.formula1pw.dfs.core.windows.net", "org.apache.hadoop.fs.azurebfs.sas.FixedSASTokenProvider")
spark.conf.set("fs.azure.sas.fixed.token.formula1pw.dfs.core.windows.net", formula1_sas_token)

# COMMAND ----------

display(dbutils.fs.ls("abfss://demo@formula1pw.dfs.core.windows.net"))

# COMMAND ----------

display(spark.read.csv("abfss://demo@formula1pw.dfs.core.windows.net/circuits.csv"))

# COMMAND ----------


