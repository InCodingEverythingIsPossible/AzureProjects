# Databricks notebook source
# MAGIC %md
# MAGIC # Spark Filter Introduction

# COMMAND ----------

# MAGIC %md
# MAGIC # Importing configuration notebooks for generic cases

# COMMAND ----------

# MAGIC %run "../Includes/configuration"

# COMMAND ----------

# MAGIC %md
# MAGIC # Read Parquet file using spark dataframe reader

# COMMAND ----------

races_df = spark.read.parquet(f"{processed_folder_path}/races")

# COMMAND ----------

# MAGIC %md
# MAGIC # Filter dataframe using SQL structure

# COMMAND ----------

races_filtered_df = races_df.filter("race_year = 2019 and round <= 5")

# COMMAND ----------

races_filtered_df = races_df.where("race_year = 2019 and round <= 5")

# COMMAND ----------

# MAGIC %md
# MAGIC # Filter dataframe using Python structure

# COMMAND ----------

races_filtered_df = races_df.filter((races_df.race_year == 2019) & (races_df.round <= 5))

# COMMAND ----------

races_filtered_df = races_df.where((races_df.race_year == 2019) & (races_df.round <= 5))

# COMMAND ----------

races_filtered_df = races_df.filter((races_df["race_year"] == 2019) & (races_df["round"] <= 5))

# COMMAND ----------

races_filtered_df = races_df.where((races_df["race_year"] == 2019) & (races_df["round"] <= 5))

# COMMAND ----------

display(races_filtered_df)
