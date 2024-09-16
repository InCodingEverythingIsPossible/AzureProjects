# Databricks notebook source
# MAGIC %md
# MAGIC # Ingest pit_stops.json file

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
# MAGIC # Read the JSON (multi line json file) using the spark dataframe reader

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, IntegerType, StringType

pit_stops_schema = StructType(fields=[
    StructField("raceId", IntegerType(), False),
    StructField("driverId", IntegerType(), True),
    StructField("stop", StringType(), True),
    StructField("lap", IntegerType(), True),
    StructField("time", StringType(), True),
    StructField("duration", StringType(), True),
    StructField("milliseconds", IntegerType(), True),
])

# COMMAND ----------

pit_stops_df = spark.read \
    .schema(pit_stops_schema) \
    .option("multiline", True) \
    .json(f"{raw_folder_path}/{file_date}/pit_stops.json")


# COMMAND ----------

display(pit_stops_df)

# COMMAND ----------

# MAGIC %md
# MAGIC # Rename columns and add ingestion date

# COMMAND ----------

pit_stops_ingestion_date_df = add_ingestion_date(pit_stops_df)

# COMMAND ----------

from pyspark.sql.functions import lit

pit_stops_final_df = pit_stops_ingestion_date_df.withColumnRenamed("driverId", "driver_id") \
                                    .withColumnRenamed("raceId", "race_id") \
                                    .withColumn("data_source", lit(data_source)) \
                                    .withColumn("file_date", lit(file_date))              

# COMMAND ----------

display(pit_stops_final_df)

# COMMAND ----------

# MAGIC %md
# MAGIC # Incremental load of the dataframe to the container on Parquet
# MAGIC - 1st cell -> function which sort dataframe (partition field need to be the last in the dataframe)
# MAGIC - 2nd cell -> function which createTable or overwrite partition specified by input 

# COMMAND ----------

# sorted_df = sortForIncrementalLoad(input_df=pit_stops_final_df, partitionField="race_id")

# COMMAND ----------

# incrementalLoad(input_df=sorted_df, databaseName="f1_processed", tableName="pit_stops", partitionField="race_id")

# COMMAND ----------

# display(spark.read.parquet(f"{processed_folder_path}/pit_stops"))

# COMMAND ----------

# MAGIC %md
# MAGIC # Incremental load of the dataframe to the container on Delta file
# MAGIC - call a function which create managed table or merge data (insert or update data based on merge key)

# COMMAND ----------

mergeCondition = """target.race_id = source.race_id AND 
                    target.driver_id = source.driver_id AND
                    target.stop = source.stop"""

incrementalLoadDelta(input_df=pit_stops_final_df, databaseName="f1_processed", tableName="pit_stops", 
                     folderPath=processed_folder_path, partitionField="race_id", mergeCondition=mergeCondition)

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * 
# MAGIC FROM f1_processed.pit_stops;

# COMMAND ----------

# MAGIC %md
# MAGIC # Returns the value 'Success' to the place where notebook is invoked

# COMMAND ----------

dbutils.notebook.exit("Success")

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT race_id, COUNT(1)
# MAGIC FROM f1_processed.pit_stops
# MAGIC GROUP BY race_id
# MAGIC ORDER BY race_id DESC;
