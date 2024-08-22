# Databricks notebook source
# MAGIC %md
# MAGIC # Databricks Utilities
# MAGIC ## dbutils command
# MAGIC - Always use in programming
# MAGIC
# MAGIC ## %fs command
# MAGIC - Always use in ad-hoc  

# COMMAND ----------

# MAGIC %fs
# MAGIC ls /databricks-datasets

# COMMAND ----------

dbutils.fs.ls('/databricks-datasets/COVID')

# COMMAND ----------

for files in dbutils.fs.ls('/databricks-datasets/COVID'):
    print(files)

# COMMAND ----------

for files in dbutils.fs.ls('/databricks-datasets/COVID'):
    if files.name.endswith('/'):
        print(files.name)

# COMMAND ----------

dbutils.help()

# COMMAND ----------

dbutils.fs.help()

# COMMAND ----------

dbutils.fs.help('ls')
