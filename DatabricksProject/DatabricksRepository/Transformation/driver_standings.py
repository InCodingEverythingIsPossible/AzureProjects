# Databricks notebook source
# MAGIC %md
# MAGIC # Produce driver standings

# COMMAND ----------

# MAGIC %md
# MAGIC # Passing parameters to the notebook

# COMMAND ----------

dbutils.widgets.text("file_date", "2021-03-21")
file_date = dbutils.widgets.get("file_date")

# COMMAND ----------

# MAGIC %md
# MAGIC # Importing configuration notebook for generic cases

# COMMAND ----------

# MAGIC %run "../Includes/configuration"

# COMMAND ----------

# MAGIC %run "../Includes/common_functions"

# COMMAND ----------

# MAGIC %md
# MAGIC # Read Parquet file using spark dataframe reader
# MAGIC - 1st cell -> find a race year for which the data is to be processed
# MAGIC - 2nd cell -> convert list of rows to list which contain only race_year
# MAGIC - 3rd cell -> read a parquet file using filter

# COMMAND ----------

race_results_list = spark.read.format("delta")\
                              .load(f"{presentation_folder_path}/race_results") \
                              .filter(f"file_date = '{file_date}'") \
                              .select("race_year") \
                              .distinct() \
                              .collect()

# COMMAND ----------

race_year_list = [row.race_year for row in race_results_list]

# COMMAND ----------

from pyspark.sql.functions import col

race_results_df = spark.read.format("delta") \
                            .load(f"{presentation_folder_path}/race_results") \
                            .filter(col("race_year").isin(race_year_list))

# COMMAND ----------

display(race_results_df)

# COMMAND ----------

# MAGIC %md
# MAGIC # Aggregation of data in a dataframe

# COMMAND ----------

from pyspark.sql.functions import sum, when, count

driver_standings_df = race_results_df.groupBy("race_year", "driver_name", "driver_nationality") \
                                     .agg(sum("points").alias("total_points"),
                                          count(when(col("position") == 1, True)).alias("wins")
                                          )

# COMMAND ----------

# MAGIC %md
# MAGIC # Ranking assignment in each season using Window 

# COMMAND ----------

from pyspark.sql.window import Window
from pyspark.sql.functions import desc, rank

driver_rank_spec = Window.partitionBy("race_year") \
                         .orderBy(desc("total_points"), desc("wins"))

final_df = driver_standings_df.withColumn("rank", rank().over(driver_rank_spec))

# COMMAND ----------

# MAGIC %md
# MAGIC # Write data to DataLake as Parquet

# COMMAND ----------

# sorted_df = sortForIncrementalLoad(input_df=final_df, partitionField="race_year")

# COMMAND ----------

# incrementalLoad(input_df=sorted_df, databaseName="f1_presentation", tableName="driver_standings", partitionField="race_year")

# COMMAND ----------

# display(spark.read.parquet(f"{presentation_folder_path}/driver_standings"))

# COMMAND ----------

# MAGIC %md
# MAGIC # Write data to DataLake in Delta format

# COMMAND ----------

mergeCondition = "target.driver_name = source.driver_name AND target.race_year = source.race_year"

incrementalLoadDelta(input_df=final_df, databaseName="f1_presentation", tableName="driver_standings", 
                     folderPath=presentation_folder_path, partitionField="race_year", mergeCondition=mergeCondition)

# COMMAND ----------

display(spark.read.format("delta") \
                  .load(f"{presentation_folder_path}/driver_standings"))

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT *
# MAGIC FROM f1_presentation.driver_standings
# MAGIC WHERE race_year = 2021;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT race_year, COUNT(1) 
# MAGIC FROM f1_presentation.driver_standings
# MAGIC GROUP BY race_year
# MAGIC ORDER BY race_year DESC;
# MAGIC
