# Databricks notebook source
# MAGIC %md
# MAGIC # Notebooks introduction
# MAGIC
# MAGIC ## Magic commands
# MAGIC - %python
# MAGIC - %sql
# MAGIC - %scala
# MAGIC - %md -> make notes
# MAGIC - %fs -> linux commands
# MAGIC - %sh
# MAGIC - %run -> run other notebooks
# MAGIC
# MAGIC ## Md cheat sheet
# MAGIC - https://www.markdownguide.org/cheat-sheet/

# COMMAND ----------

message = 'Welcome to the Databricks Notebook Experience'

# COMMAND ----------

print(message)

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT "HELLO"

# COMMAND ----------

# MAGIC %scala
# MAGIC val mess = "Hello"
# MAGIC print(mess)

# COMMAND ----------

# MAGIC %fs
# MAGIC ls

# COMMAND ----------

# MAGIC %sh
# MAGIC ps
