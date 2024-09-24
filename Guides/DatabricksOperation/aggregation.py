# Databricks notebook source
# MAGIC %md 
# MAGIC # Spark aggregation introduction

# COMMAND ----------

# MAGIC %md
# MAGIC # Importing configuration notebook for generic cases

# COMMAND ----------

# MAGIC %run "../Includes/configuration"

# COMMAND ----------

# MAGIC %md
# MAGIC # Read Parquet file using spark dataframe reader

# COMMAND ----------

race_results_df = spark.read.parquet(f"{presentation_folder_path}/race_results") \
                            .filter("race_year in (2019,2020)")

# COMMAND ----------

display(race_results_df)

# COMMAND ----------

# MAGIC %md
# MAGIC # Sample aggregation
# MAGIC - count
# MAGIC - countDistinct
# MAGIC - sum

# COMMAND ----------

from pyspark.sql.functions import count, countDistinct, sum

# COMMAND ----------

race_results_df.select(count("*").alias("number_of_records")) \
                .show()

# COMMAND ----------

race_results_df.select(count("race_name").alias("number_of_records")) \
                .show()

# COMMAND ----------

race_results_df.select(countDistinct("race_name").alias("distinct_races")) \
                .show()

# COMMAND ----------

race_results_df.select(sum("points").alias("total_points"))\
                .show()

# COMMAND ----------

race_results_df.filter("driver_name = 'Lewis Hamilton'") \
                .select(sum("points").alias("hamilton_points")) \
                .show() 

# COMMAND ----------

race_results_df.filter("driver_name = 'Lewis Hamilton'") \
                .select(sum("points").alias("total_points"), countDistinct("race_name").alias("number_of_races")) \
                .show() 

# COMMAND ----------

# MAGIC %md
# MAGIC # Grouping data
# MAGIC - 1st we can use only one aggregation function
# MAGIC - 2nd we can use as many aggegration functions as we need in that structure

# COMMAND ----------

race_results_df.groupBy("driver_name") \
                .sum("points").alias("total_points") \
                .show()

# COMMAND ----------

race_results_df.groupBy("driver_name") \
                .agg(sum("points").alias("total_points"), countDistinct("race_name").alias("number_of_races")) \
                .show()

# COMMAND ----------

# MAGIC %md
# MAGIC # Window Functions
# MAGIC - it returns value for each row in a specific group of rows from the dataframe
# MAGIC - for example -> assigns ranking positions to people taking part in specific race

# COMMAND ----------

race_grouped_df = race_results_df.groupBy("race_year", "driver_name") \
                                .agg(sum("points").alias("total_points"), countDistinct("race_name").alias("number_of_races")) 

# COMMAND ----------

display(race_grouped_df)

# COMMAND ----------

from pyspark.sql.window import Window
from pyspark.sql.functions import desc, rank

driverRankSpec = Window.partitionBy("race_year") \
                        .orderBy(desc("total_points"))

race_grouped_df.withColumn("rank",rank().over(driverRankSpec)) \
                .show()
