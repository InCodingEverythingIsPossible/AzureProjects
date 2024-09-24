# Databricks notebook source
# MAGIC %md
# MAGIC # Access Azure Data Lake using Service Principal
# MAGIC
# MAGIC ## List of tasks to do
# MAGIC - Register Azure AD Application / Service Principal
# MAGIC   1. Search for Microsoft Entra ID
# MAGIC   2. App registrations (left of the list)
# MAGIC   3. Click new registration
# MAGIC   4. Copy client_id and tenant_id from overview window of registration app
# MAGIC   5. Click on Certificates & Secrets in app menu
# MAGIC   6. Create a new client secret and copy that value
# MAGIC - Create Azure Key Vault (if it is not created for the project)
# MAGIC     1. Create Azure Key Vault Service
# MAGIC     2. Go to Secrets Session (left of the list Objects section)
# MAGIC     3. Click Generate/Import
# MAGIC     4. Enter a values and create secret
# MAGIC
# MAGIC - Assign Role 'Storage Blob Data Contributor' to the Data Lake in IAM of Storage Account for the application
# MAGIC   1. Go to the Access Control (IAM) of Storage Account
# MAGIC   2. Click Add -> choose Add role assignment
# MAGIC   3. Find 'Storage Blob Data Contributor' from the list and click next
# MAGIC   4. Grant access to application in select member option
# MAGIC
# MAGIC - Connect Secret scope on Databricks with Azure Key Vault (if it is not connected to Azure Key Vault)
# MAGIC     1. Go to the main page of Databricks
# MAGIC     2. In the url at the end add -> #secrets/createScope
# MAGIC     3. Copy from Azure Key Vault properties DNS Name (Vault URI) and Resource ID
# MAGIC     
# MAGIC - Set Spark config with App / Client Id, Directory / Tenant Id & Secret
# MAGIC

# COMMAND ----------

client_id = dbutils.secrets.get(scope = "formula1-scope", key = "formula1pw-client-id")
tenant_id = dbutils.secrets.get(scope = "formula1-scope", key = "formula1pw-tenant-id")
client_secret = dbutils.secrets.get(scope = "formula1-scope", key = "formula1pw-client-secret")

# COMMAND ----------

spark.conf.set("fs.azure.account.auth.type.formula1pw.dfs.core.windows.net", "OAuth")
spark.conf.set("fs.azure.account.oauth.provider.type.formula1pw.dfs.core.windows.net", "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider")
spark.conf.set("fs.azure.account.oauth2.client.id.formula1pw.dfs.core.windows.net", client_id)
spark.conf.set("fs.azure.account.oauth2.client.secret.formula1pw.dfs.core.windows.net", client_secret)
spark.conf.set("fs.azure.account.oauth2.client.endpoint.formula1pw.dfs.core.windows.net", f"https://login.microsoftonline.com/{tenant_id}/oauth2/token")

# COMMAND ----------

display(dbutils.fs.ls("abfss://demo@formula1pw.dfs.core.windows.net"))

# COMMAND ----------

display(spark.read.csv("abfss://demo@formula1pw.dfs.core.windows.net/circuits.csv"))

# COMMAND ----------


