# Databricks notebook source
# MAGIC %md
# MAGIC # Explore DBFS Root
# MAGIC - List all the folders in DBFS Root
# MAGIC - Interact with DBFS File Browser
# MAGIC   1. Click on user icon and go to the settings
# MAGIC   2. Go to the 'Advanced' in Workspace Admin section
# MAGIC   3. Click toggle switch on 'DBFS File Browser' and refresh page
# MAGIC   4. In Catalog section of Databricks in the top of the screen new option will appear called 'Browse DBFS'
# MAGIC - Upload file to DBFS Root

# COMMAND ----------

display(dbutils.fs.ls('/'))

# COMMAND ----------

display(dbutils.fs.ls('/FileStore'))

# COMMAND ----------

display(spark.read.csv('dbfs:/FileStore/circuits.csv'))

# COMMAND ----------


