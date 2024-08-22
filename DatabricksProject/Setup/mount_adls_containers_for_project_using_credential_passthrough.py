# Databricks notebook source
# MAGIC %md
# MAGIC # Mount Azure Data Lake Containers using credential passthrough
# MAGIC ### For more details go to the 'Mount_adls_using_credential_passthrough'

# COMMAND ----------

def mount_adls(storage_account_name, container_name):

    #Set spark configuration
    configs = {
    "fs.azure.account.auth.type": "CustomAccessToken",
    "fs.azure.account.custom.token.provider.class" : spark.conf.get("spark.databricks.passthrough.adls.gen2.tokenProviderClassName")
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
