# Databricks notebook source
# MAGIC %md
# MAGIC # Produce race results

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
# MAGIC # Read Parquet files using spark dataframe reader
# MAGIC - readed all files
# MAGIC - selected only necessary fields
# MAGIC - filtered data 

# COMMAND ----------

from pyspark.sql.functions import col, current_timestamp

# COMMAND ----------

circuits_df = spark.read.format("delta") \
                        .load(f"{processed_folder_path}/circuits") \
                        .select("circuit_id", col("location").alias("circuit_location"))

# COMMAND ----------

races_df = spark.read.format("delta") \
                     .load(f"{processed_folder_path}/races") \
                     .select("race_id", col("race_year"), col("name").alias("race_name"), col("race_timestamp").alias("race_date"), "circuit_id")
                       

# COMMAND ----------

drivers_df = spark.read.format("delta") \
                       .load(f"{processed_folder_path}/drivers") \
                       .select("driver_id", col("number").alias("driver_number"), col("name").alias("driver_name"), 
                                col("nationality").alias("driver_nationality"))

# COMMAND ----------

constructors_df = spark.read.format("delta") \
                            .load(f"{processed_folder_path}/constructors") \
                            .select("constructor_id", col("name").alias("team"))

# COMMAND ----------

results_df = spark.read.format("delta") \
                       .load(f"{processed_folder_path}/results") \
                       .filter(f"file_date = '{file_date}'") \
                       .withColumn("created_date",current_timestamp()) \
                       .select("result_id", col("race_id").alias("results_race_id"), "driver_id", "constructor_id","grid", "fastest_lap", "points", 
                                "position", col("time").alias("race_time"), "created_date", "file_date")
                        

# COMMAND ----------

# MAGIC %md 
# MAGIC # Join all data to receive final dataframe
# MAGIC - 1st, 2nd cell -> join data in two steps to make more readable 
# MAGIC - 3rd cell -> join data in one step

# COMMAND ----------

races_circuits_df = races_df.join(circuits_df, races_df.circuit_id == circuits_df.circuit_id, "inner") \
                            .drop(races_df.circuit_id, circuits_df.circuit_id)

# COMMAND ----------

final_df = results_df.join(races_circuits_df, results_df.results_race_id == races_circuits_df.race_id, "inner") \
                        .join(drivers_df, results_df.driver_id == drivers_df.driver_id, "inner") \
                        .join(constructors_df, results_df.constructor_id == constructors_df.constructor_id, "inner") \
                        .select("race_year", "race_name", "race_date", "circuit_location", "driver_name", "driver_number",
                                "driver_nationality", "team", "grid", "fastest_lap", "race_time", "points", "position", "created_date", 
                                "race_id", "file_date")

# COMMAND ----------

final_df = results_df.join(races_df.join( circuits_df, races_df.circuit_id == circuits_df.circuit_id, "inner") \
                                    .drop(races_df.circuit_id, circuits_df.circuit_id)
                            ,results_df.results_race_id == races_df.race_id, "inner") \
                        .join(drivers_df, results_df.driver_id == drivers_df.driver_id, "inner") \
                        .join(constructors_df, results_df.constructor_id == constructors_df.constructor_id, "inner") \
                        .select("race_year", "race_name", "race_date", "circuit_location", "driver_name", "driver_number",
                                "driver_nationality", "team", "grid", "fastest_lap", "race_time", "points", "position", "created_date", 
                                "race_id", "file_date")

# COMMAND ----------

# MAGIC %md
# MAGIC # Filter and sort data to receive expected result

# COMMAND ----------

abu_dhabi_df = final_df.filter(final_df.race_name == "Abu Dhabi Grand Prix") \
                        .orderBy(final_df.points.desc())

# COMMAND ----------

abu_dhabi_df = final_df.filter(final_df.race_name == "Abu Dhabi Grand Prix") \
                        .sort(final_df.points.desc())

# COMMAND ----------

abu_dhabi_df = final_df.filter(final_df.race_name == "Abu Dhabi Grand Prix") \
                        .sort(final_df.points, ascending=False)

# COMMAND ----------

display(abu_dhabi_df)

# COMMAND ----------

# MAGIC %md
# MAGIC # Write data to DataLake as Parquet

# COMMAND ----------

# sorted_df = sortForIncrementalLoad(input_df=final_df, partitionField="race_id")

# COMMAND ----------

# incrementalLoad(input_df=sorted_df, databaseName="f1_presentation", tableName="race_results", partitionField="race_id")

# COMMAND ----------

# display(spark.read.parquet(f"{presentation_folder_path}/race_results"))

# COMMAND ----------

# MAGIC %md
# MAGIC # Write data to DataLake in Delta format

# COMMAND ----------

mergeCondition = "target.driver_name = source.driver_name AND target.race_id = source.race_id"

incrementalLoadDelta(input_df=final_df, databaseName="f1_presentation", tableName="race_results", 
                     folderPath=presentation_folder_path, partitionField="race_id", mergeCondition=mergeCondition)

# COMMAND ----------

display(spark.read.format("delta") \
                  .load(f"{presentation_folder_path}/race_results"))

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT race_id, COUNT(1)
# MAGIC FROM f1_presentation.race_results
# MAGIC GROUP BY race_id
# MAGIC ORDER BY race_id DESC;
