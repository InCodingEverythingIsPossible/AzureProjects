-- Databricks notebook source
-- MAGIC %md
-- MAGIC #Access External Datalake
-- MAGIC - Create Storage Credential
-- MAGIC - Create External Location
-- MAGIC

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #Create Storage Credential
-- MAGIC
-- MAGIC ### Create the necessary tools 
-- MAGIC - Create Access Connector for Azure Databricks
-- MAGIC - Create Storage Account 
-- MAGIC
-- MAGIC
-- MAGIC ### Grant permission for Azure Databricks to DataLake
-- MAGIC - Go to the Access Control (IAM) of Storage Account
-- MAGIC - Click Add -> choose Add role assignment
-- MAGIC - Find 'Storage Blob Data Contributor' from the list and click next
-- MAGIC - Choose checkbox Managed Identity
-- MAGIC - Grant access to Access Connector for Azure Databricks in select member option
-- MAGIC
-- MAGIC ### Create Storage Credential
-- MAGIC - Click External Data in Catalog Section of Databricks (at the top of the screen)
-- MAGIC - Go to Storage Credentials window
-- MAGIC - Click Create credential (top right corner)
-- MAGIC - Enter name of Storage Credential
-- MAGIC - Paste Access Connector Id -> In Azure Portal go to Access Connector which we created, enter Settings/Properties Section and copy Id
-- MAGIC - Click Create

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #Create External Location
-- MAGIC - Click External Data in Catalog Section of Databricks (at the top of the screen)
-- MAGIC - Go to External Locations window
-- MAGIC - Click Create location (top right conrner)
-- MAGIC - Enter name of External location
-- MAGIC - Paste URL of datalake in format -> abfss://{container}@{storage_account}.dfs.core.windows.net/
-- MAGIC - Choose Storage Credential which you created earlier
-- MAGIC - Click Create
