# Databricks notebook source
# MAGIC %md
# MAGIC # Ingest races.csv file

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
# MAGIC # Read CSV (single file) using spark dataframe reader

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DateType

races_schema = StructType(fields=[
    StructField("raceId", IntegerType(), False),
    StructField("year", IntegerType(), True),
    StructField("round", IntegerType(), True),
    StructField("circuitId", IntegerType(), True),
    StructField("name", StringType(), True),
    StructField("date", DateType(), True),
    StructField("time", StringType(), True),
    StructField("url", StringType(), True)
])

# COMMAND ----------

races_df = spark.read \
    .option("header", True) \
    .schema(races_schema) \
    .csv(f"{raw_folder_path}/races.csv")

# COMMAND ----------

display(races_df)

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC # Add ingestion date and race timestamp to the dataframe

# COMMAND ----------

races_ingestion_date_df = add_ingestion_date(races_df)

# COMMAND ----------

from pyspark.sql.functions import lit, concat, to_timestamp, col

races_new_fields_df = races_ingestion_date_df.withColumn("race_timestamp", to_timestamp(concat(col("date"),lit(" "), col("time")), "yyyy-MM-dd HH:mm:ss")) \
                                                .withColumn("data_source", lit(data_source)) 
                    

# COMMAND ----------

display(races_new_fields_df.printSchema())

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC # Select only required fields in the dataframe

# COMMAND ----------

from pyspark.sql.functions import col

races_final_df = races_new_fields_df.select(col("raceId").alias("race_id"), col("year").alias("race_year"), col("round"), 
                                            col("circuitId").alias("circuit_id"), col("name"), col("race_timestamp"), col("ingestion_date"), col("data_source"))

# COMMAND ----------

display(races_final_df)

# COMMAND ----------

# MAGIC %md 
# MAGIC
# MAGIC # Write dataframe to the container in Parquet format partitioned by year

# COMMAND ----------

races_final_df.write.mode("overwrite") \
                    .partitionBy("race_year") \
                    .parquet(f"{processed_folder_path}/races")

# COMMAND ----------

display(spark.read.parquet(f"{processed_folder_path}/races"))

# COMMAND ----------

# MAGIC %md
# MAGIC # Returns the value 'Success' to the place where notebook is invoked

# COMMAND ----------

dbutils.notebook.exit("Success")

# COMMAND ----------

# MAGIC %md
