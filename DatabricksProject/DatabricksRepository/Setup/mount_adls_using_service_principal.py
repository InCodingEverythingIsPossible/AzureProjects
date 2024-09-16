# Databricks notebook source
# MAGIC %md
# MAGIC # Mount Azure Data Lake using Service Principal
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
# MAGIC - Call file system utility mount to mount the storage
# MAGIC - Explore other file system utilities related to mount (list all mounts, unmount)

# COMMAND ----------

client_id = dbutils.secrets.get(scope = "formula1-scope", key = "formula1pw-client-id")
tenant_id = dbutils.secrets.get(scope = "formula1-scope", key = "formula1pw-tenant-id")
client_secret = dbutils.secrets.get(scope = "formula1-scope", key = "formula1pw-client-secret")

# COMMAND ----------

configs = {
    "fs.azure.account.auth.type": "OAuth",
    "fs.azure.account.oauth.provider.type": "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider",
    "fs.azure.account.oauth2.client.id": client_id,
    "fs.azure.account.oauth2.client.secret": client_secret,
    "fs.azure.account.oauth2.client.endpoint": f"https://login.microsoftonline.com/{tenant_id}/oauth2/token"
}

# COMMAND ----------

dbutils.fs.mount(
  source = "abfss://demo@formula1pw.dfs.core.windows.net/",
  mount_point = "/mnt/formula1pw/demo",
  extra_configs = configs)


# COMMAND ----------

display(dbutils.fs.ls("/mnt/formula1pw/demo"))

# COMMAND ----------

display(spark.read.csv("/mnt/formula1pw/demo/circuits.csv"))

# COMMAND ----------

display(dbutils.fs.mounts())

# COMMAND ----------

dbutils.fs.unmount('/mnt/formula1pw/demo')
