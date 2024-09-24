# Databricks notebook source
# MAGIC %md
# MAGIC #Initial configuration
# MAGIC
# MAGIC ### Create resource group
# MAGIC - Create Storage Account
# MAGIC - Create Databricks Workspace
# MAGIC - Create Access Connector for Azure Databricks
# MAGIC
# MAGIC ### Grant permission for Azure Databricks to DataLake
# MAGIC - Go to the Access Control (IAM) of Storage Account
# MAGIC - Click Add -> choose Add role assignment
# MAGIC - Find 'Storage Blob Data Contributor' from the list and click next
# MAGIC - Choose checkbox Managed Identity
# MAGIC - Grant access to Access Connector for Azure Databricks in select member option
# MAGIC
# MAGIC ### Enable Unity Catalog Metastore (only one per region)
# MAGIC - Click on your icon in Databricks (top right corner)
# MAGIC - Click Manage Account
# MAGIC - Go to the Data Section and click Create metastore 
# MAGIC - Paste ADLS Gen 2 path in that format -> container@storage_account_name/ 
# MAGIC - Paste Access Connector Id -> In Azure Portal go to Access Connector which we created, enter Settings/Properties Section and copy Id
# MAGIC - Click create button
# MAGIC - Assign workspaces for which use Unity Catalog Metastore
# MAGIC
# MAGIC ### Cluster Configuration for Unity Catalog Metastore
# MAGIC - During creating cluster ensure that in summary you are able to connect to unity catalog
