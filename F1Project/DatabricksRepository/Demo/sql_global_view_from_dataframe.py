# Databricks notebook source
# MAGIC %md
# MAGIC # Access dataframes using SQL
# MAGIC
# MAGIC ### Objectives
# MAGIC - Create global temporary views on dataframes -> only available in current session of cluster from any notebook which use same cluster
# MAGIC - Access the view from SQL cell -> use for analyze
# MAGIC - Access the view from Python cell -> use to put in the dataframe, use to pass the argument

# COMMAND ----------

# MAGIC %md
# MAGIC # Importing configuration notebook for generic cases

# COMMAND ----------

# MAGIC %run "../Includes/configuration"

# COMMAND ----------

# MAGIC %md
# MAGIC # Read Parquet file using spark dataframe reader

# COMMAND ----------

race_results_df = spark.read.parquet(f"{presentation_folder_path}/race_results")

# COMMAND ----------

display(race_results_df)

# COMMAND ----------

# MAGIC %md
# MAGIC # Create global temporary views on dataframes
# MAGIC - 1st cell -> create global temporary view
# MAGIC - 2nd cell -> replace global temporary view

# COMMAND ----------

race_results_df.createGlobalTempView("view_race_results")

# COMMAND ----------

race_results_df.createOrReplaceGlobalTempView("view_race_results")

# COMMAND ----------

# MAGIC %md
# MAGIC # Access the view from SQL cell

# COMMAND ----------

# MAGIC %sql
# MAGIC SHOW TABLES IN global_temp;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * 
# MAGIC FROM global_temp.view_race_results

# COMMAND ----------

# MAGIC %md
# MAGIC # Access the view from Python cell

# COMMAND ----------

p_race_year = 2019

# COMMAND ----------

race_results_2019_df = spark.sql(f"""SELECT * 
                                FROM global_temp.view_race_results
                                WHERE race_year = {p_race_year}""")

# COMMAND ----------

display(race_results_2019_df)
