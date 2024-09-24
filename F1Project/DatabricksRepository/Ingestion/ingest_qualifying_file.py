# Databricks notebook source
# MAGIC %md
# MAGIC # Ingest qualifying.json file

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
# MAGIC # Read the JSON (multi file with multi line json file) using the spark dataframe reader

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, IntegerType, StringType

qualifying_schema = StructType(fields=[
    StructField("qualifyId", IntegerType(), False),
    StructField("raceId", IntegerType(), True),
    StructField("driverId", IntegerType(), True),
    StructField("constructorId", IntegerType(), True),
    StructField("number", IntegerType(), True),
    StructField("position", IntegerType(), True),
    StructField("q1", StringType(), True),
    StructField("q2", StringType(), True),
    StructField("q3", StringType(), True),
])

# COMMAND ----------

qualifying_df = spark.read \
    .schema(qualifying_schema) \
    .option("multiline", True) \
    .json(f"{raw_folder_path}/{file_date}/qualifying")


# COMMAND ----------

display(qualifying_df)

# COMMAND ----------

# MAGIC %md
# MAGIC # Rename columns and add ingestion date

# COMMAND ----------

qualifying_df.printSchema()

# COMMAND ----------

qualifying_ingestion_date_df = add_ingestion_date(qualifying_df)

# COMMAND ----------

from pyspark.sql.functions import lit

qualifying_final_df = qualifying_ingestion_date_df.withColumnRenamed("qualifyId", "qualify_id") \
                                    .withColumnRenamed("raceId", "race_id") \
                                    .withColumnRenamed("driverId", "driver_id") \
                                    .withColumnRenamed("constructorId", "constructor_id") \
                                    .withColumn("data_source", lit(data_source)) \
                                    .withColumn("file_date", lit(file_date))           

# COMMAND ----------

display(qualifying_final_df)

# COMMAND ----------

# MAGIC %md
# MAGIC # Incremental load of the dataframe to the container
# MAGIC - 1st cell -> function which sort dataframe (partition field need to be the last in the dataframe)
# MAGIC - 2nd cell -> function which createTable or overwrite partition specified by input 

# COMMAND ----------

# sorted_df = sortForIncrementalLoad(input_df=qualifying_final_df, partitionField="race_id")

# COMMAND ----------

# incrementalLoad(input_df=sorted_df, databaseName="f1_processed", tableName="qualifying", partitionField="race_id")

# COMMAND ----------

# display(spark.read.parquet(f"{processed_folder_path}/qualifying"))

# COMMAND ----------

# MAGIC %md
# MAGIC # Incremental load of the dataframe to the container on Delta file
# MAGIC - call a function which create managed table or merge data (insert or update data based on merge key)

# COMMAND ----------

mergeCondition = """target.qualify_id = source.qualify_id AND
                    target.race_id = source.race_id"""

incrementalLoadDelta(input_df=qualifying_final_df, databaseName="f1_processed", tableName="qualifying", 
                     folderPath=processed_folder_path, partitionField="race_id", mergeCondition=mergeCondition)

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * 
# MAGIC FROM f1_processed.qualifying;

# COMMAND ----------

# MAGIC %md
# MAGIC # Returns the value 'Success' to the place where notebook is invoked

# COMMAND ----------

dbutils.notebook.exit("Success")

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT race_id, COUNT(1)
# MAGIC FROM f1_processed.qualifying
# MAGIC GROUP BY race_id
# MAGIC ORDER BY race_id DESC;
