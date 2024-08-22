# Databricks notebook source
# MAGIC %md
# MAGIC # Ingest lap_times.json file

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
# MAGIC # Read the CSV (multi files) using the spark dataframe reader

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, IntegerType, StringType

lap_times_schema = StructType(fields=[
    StructField("raceId", IntegerType(), False),
    StructField("driverId", IntegerType(), True),
    StructField("lap", IntegerType(), True),
    StructField("position", IntegerType(), True),
    StructField("time", StringType(), True),
    StructField("milliseconds", IntegerType(), True),
])

# COMMAND ----------

# MAGIC %md
# MAGIC # Posibilities to collect files from folder
# MAGIC - .csv("/mnt/formula1pw/bronze/lap_times") -> to collect all files from folder
# MAGIC - .csv("/mnt/formula1pw/bronze/lap_times/lap_times_split*.csv") -> to collect files with specific pattern

# COMMAND ----------

lap_times_df = spark.read \
    .schema(lap_times_schema) \
    .csv(f"{raw_folder_path}/lap_times")


# COMMAND ----------

display(lap_times_df)

# COMMAND ----------

# MAGIC %md
# MAGIC # Rename columns and add ingestion date

# COMMAND ----------

lap_times_ingestion_date_df = add_ingestion_date(lap_times_df)

# COMMAND ----------

from pyspark.sql.functions import lit

lap_times_final_df = lap_times_ingestion_date_df.withColumnRenamed("driverId", "driver_id") \
                                    .withColumnRenamed("raceId", "race_id") \
                                    .withColumn("data_source", lit(data_source))        

# COMMAND ----------

display(lap_times_final_df)

# COMMAND ----------

# MAGIC %md
# MAGIC # Write dataframe to the container in Parquet format

# COMMAND ----------

lap_times_final_df.write.mode("overwrite") \
                        .parquet(f"{processed_folder_path}/lap_times")

# COMMAND ----------

display(spark.read.parquet(f"{processed_folder_path}/lap_times"))

# COMMAND ----------

# MAGIC %md
# MAGIC # Returns the value 'Success' to the place where notebook is invoked

# COMMAND ----------

dbutils.notebook.exit("Success")
