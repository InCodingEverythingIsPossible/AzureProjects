# Databricks notebook source
# MAGIC %md
# MAGIC # Ingest results.json file

# COMMAND ----------

# MAGIC %md
# MAGIC # Passing parameters to the notebook

# COMMAND ----------

dbutils.widgets.text("data_source", "")
data_source = dbutils.widgets.get("data_source")

# COMMAND ----------

dbutils.widgets.text("file_date", "2021-03-28")
file_date = dbutils.widgets.get("file_date")

# COMMAND ----------

# MAGIC %md
# MAGIC # Importing configuration and common_functions notebooks for generic cases

# COMMAND ----------

# MAGIC %run "../Includes/configuration"

# COMMAND ----------

# MAGIC %run "../Includes/common_functions"

# COMMAND ----------

# MAGIC %md
# MAGIC # Read the JSON (single json file) using the spark dataframe reader

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DoubleType

results_schema = StructType(fields=[
    StructField("constructorId", IntegerType(), False),
    StructField("driverId", IntegerType(), True),
    StructField("fastestLap", StringType(), True),
    StructField("fastestLapSpeed", StringType(), True),
    StructField("fastestLapTime", StringType(), True),
    StructField("grid", IntegerType(), True),
    StructField("laps", IntegerType(), True),
    StructField("milliseconds", StringType(), True),
    StructField("number", StringType(), True),
    StructField("points", DoubleType(), True),
    StructField("position", StringType(), True),
    StructField("positionOrder", IntegerType(), True),
    StructField("positionText", StringType(), True),
    StructField("raceId", IntegerType(), True),
    StructField("rank", StringType(), True),
    StructField("resultId", IntegerType(), True),
    StructField("statusId", IntegerType(), True),
    StructField("time", StringType(), True)
])

# COMMAND ----------

results_df = spark.read \
    .schema(results_schema) \
    .json(f"{raw_folder_path}/{file_date}/results.json")


# COMMAND ----------

display(results_df)

# COMMAND ----------

# MAGIC %md
# MAGIC # Drop unwanted columns from the dataframe

# COMMAND ----------

from pyspark.sql.functions import col

results_dropped_df = results_df.drop(col('statusId'))

# COMMAND ----------

# MAGIC %md
# MAGIC # Rename columns and add ingestion date

# COMMAND ----------

results_ingestion_date_df = add_ingestion_date(results_dropped_df)

# COMMAND ----------

from pyspark.sql.functions import lit

results_final_df = results_ingestion_date_df.withColumnRenamed("constructorId", "constructor_id") \
                                        .withColumnRenamed("driverId", "driver_id") \
                                        .withColumnRenamed("fastestLap", "fastest_lap") \
                                        .withColumnRenamed("fastestLapSpeed", "fastest_lap_speed") \
                                        .withColumnRenamed("fastestLapTime", "fastest_lap_time") \
                                        .withColumnRenamed("positionOrder", "position_order") \
                                        .withColumnRenamed("positionText", "position_text") \
                                        .withColumnRenamed("raceId", "race_id") \
                                        .withColumnRenamed("resultId", "result_id") \
                                        .withColumn("data_source", lit(data_source)) \
                                        .withColumn("file_date", lit(file_date))            

# COMMAND ----------

# MAGIC %md
# MAGIC #Deduplication the dataframe

# COMMAND ----------

results_deduped_df = results_final_df.dropDuplicates(["race_id", "driver_id"])

# COMMAND ----------

display(results_final_df)

# COMMAND ----------

# MAGIC %md
# MAGIC # Method 1 of incremental load on Parquet file
# MAGIC - 1st cell -> drop partition if already exists in container to avoid duplicates
# MAGIC - 2nd cell -> write dataframe to the container in Parquet format with append mode
# MAGIC - less efficient 

# COMMAND ----------

# for race_id_list in results_deduped_df.select("race_id").distinct().collect():
#     if (spark._jsparkSession.catalog().tableExists("f1_processed.results")):
#         spark.sql(f"ALTER TABLE f1_processed.results DROP IF EXISTS PARTITION (race_id = {race_id_list.race_id})")

# COMMAND ----------

# results_deduped_df.write.mode("append") \
#                        .partitionBy("race_id") \
#                        .format("parquet") \
#                        .saveAsTable("f1_processed.results")

# COMMAND ----------

# MAGIC %md
# MAGIC # Method 2 of incremental load on Parquet file
# MAGIC - 1st cell -> function which sort dataframe (partition field need to be the last in the dataframe)
# MAGIC - 2nd cell -> function which create managed table or overwrite partition specified by input
# MAGIC - more efficient 

# COMMAND ----------

# MAGIC %sql
# MAGIC --DROP TABLE f1_processed.results;

# COMMAND ----------

# sorted_df = sortForIncrementalLoad(input_df=results_deduped_df, partitionField="race_id")

# COMMAND ----------

# incrementalLoad(input_df=sorted_df, databaseName="f1_processed", tableName="results", partitionField="race_id")

# COMMAND ----------

# display(spark.read.parquet(f"{processed_folder_path}/results"))

# COMMAND ----------

# MAGIC %md
# MAGIC # Method 3 of incremental load on Delta file
# MAGIC - call a function which create managed table or merge data (insert or update data based on merge key)
# MAGIC - best solution of that 3 methods

# COMMAND ----------

mergeCondition = """target.result_id = source.result_id AND 
                    target.race_id = source.race_id"""

incrementalLoadDelta(input_df=results_deduped_df, databaseName="f1_processed", tableName="results", 
                     folderPath=processed_folder_path, partitionField="race_id", mergeCondition=mergeCondition)

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * 
# MAGIC FROM f1_processed.results;

# COMMAND ----------

# MAGIC %md
# MAGIC # Returns the value 'Success' to the place where notebook is invoked

# COMMAND ----------

dbutils.notebook.exit("Success")

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT race_id, COUNT(1)
# MAGIC FROM f1_processed.results
# MAGIC GROUP BY race_id
# MAGIC ORDER BY race_id DESC;
