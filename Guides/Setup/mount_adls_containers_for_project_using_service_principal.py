# Databricks notebook source
# MAGIC %md
# MAGIC # Mount Azure Data Lake Containers using service principal
# MAGIC ### For more details go to the 'Mount_adls_using_service_principal'

# COMMAND ----------

def mount_adls(storage_account_name, container_name):
    # Get secrets from key vault
    client_id = dbutils.secrets.get(scope = "formula1-scope", key = "formula1pw-client-id")
    tenant_id = dbutils.secrets.get(scope = "formula1-scope", key = "formula1pw-tenant-id")
    client_secret = dbutils.secrets.get(scope = "formula1-scope", key = "formula1pw-client-secret")

    #Set spark configuration
    configs = {
        "fs.azure.account.auth.type": "OAuth",
        "fs.azure.account.oauth.provider.type": "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider",
        "fs.azure.account.oauth2.client.id": client_id,
        "fs.azure.account.oauth2.client.secret": client_secret,
        "fs.azure.account.oauth2.client.endpoint": f"https://login.microsoftonline.com/{tenant_id}/oauth2/token"
    }

    #Unmount the mount point if it already exists
    if any(mount.mountPoint == f"/mnt/{storage_account_name}/{container_name}" for mount in dbutils.fs.mounts()):
        dbutils.fs.unmount(f"/mnt/{storage_account_name}/{container_name}")

    #Mount the storage account container
    dbutils.fs.mount(
        source = f"abfss://{container_name}@{storage_account_name}.dfs.core.windows.net/",
        mount_point = f"/mnt/{storage_account_name}/{container_name}",
        extra_configs = configs)
    
    #List mounted containers
    display(dbutils.fs.mounts())

# COMMAND ----------

# MAGIC %md
# MAGIC ## Mount Bronze (Raw) Container

# COMMAND ----------

mount_adls('formula1pw', 'bronze')

# COMMAND ----------

# MAGIC %md
# MAGIC ## Mount Silver (Processed) Container

# COMMAND ----------

mount_adls('formula1pw', 'silver')

# COMMAND ----------

# MAGIC %md
# MAGIC ## Mount Gold (Presentation) Container

# COMMAND ----------

mount_adls('formula1pw', 'gold')
