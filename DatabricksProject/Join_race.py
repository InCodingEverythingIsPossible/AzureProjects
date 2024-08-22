# Databricks notebook source
# MAGIC %md
# MAGIC # Importing configuration and common_functions notebooks for generic cases

# COMMAND ----------

# MAGIC %run "../Includes/configuration"

# COMMAND ----------

# MAGIC %run "../Includes/common_functions"

# COMMAND ----------

# MAGIC %md
# MAGIC # Read Parquet file using spark dataframe reader

# COMMAND ----------

from pyspark.sql.functions import col

# COMMAND ----------

circuits_df = spark.read.parquet(f"{processed_folder_path}/circuits") \
                        .select("circuit_id", col("location").alias("circuit_location"))

# COMMAND ----------

races_df = spark.read.parquet(f"{processed_folder_path}/races") \
                        .filter("year = 2020") \
                        .select("race_id", col("year").alias("race_year"), col("name").alias("race_name"), col("date").alias("race_date"), "circuit_id")
                       

# COMMAND ----------

drivers_df = spark.read.parquet(f"{processed_folder_path}/drivers") \
                        .select("driver_id", col("number").alias("driver_number"), col("name").alias("driver_name"), col("nationality").alias("driver_nationality"))

# COMMAND ----------

constructors_df = spark.read.parquet(f"{processed_folder_path}/constructors") \
                        .select("constructor_id", col("name").alias("team"))

# COMMAND ----------

results_df = spark.read.parquet(f"{processed_folder_path}/results") \
                        .select("result_id","race_id","driver_id", "constructor_id","grid", "fastest_lap", "points")
