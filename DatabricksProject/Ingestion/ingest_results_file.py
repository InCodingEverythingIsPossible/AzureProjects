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
    .json(f"{raw_folder_path}/results.json")


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
                                        .withColumn("data_source", lit(data_source))            

# COMMAND ----------

display(results_final_df)

# COMMAND ----------

# MAGIC %md
# MAGIC # Write dataframe to the container in Parquet format

# COMMAND ----------

results_final_df.write.mode("overwrite") \
                        .parquet(f"{processed_folder_path}/results")

# COMMAND ----------

display(spark.read.parquet(f"{processed_folder_path}/results"))

# COMMAND ----------

# MAGIC %md
# MAGIC # Returns the value 'Success' to the place where notebook is invoked

# COMMAND ----------

dbutils.notebook.exit("Success")
