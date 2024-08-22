# Databricks notebook source
# MAGIC %md
# MAGIC # Ingest circuits.csv file
# MAGIC

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
# MAGIC
# MAGIC ### Read the CSV (single file) using the spark dataframe reader and inferSchema 
# MAGIC - spark dataframe reader auto determines datatype of fields
# MAGIC - use case -> test environment

# COMMAND ----------

circuits_df = spark.read \
    .option("header", True) \
    .option("inferSchema", True) \
    .csv(f"{raw_folder_path}/circuits.csv")

# COMMAND ----------

# MAGIC %md 
# MAGIC
# MAGIC ### Read the CSV file using the spark dataframe reader and DDL schema
# MAGIC - define fields ourselves
# MAGIC - use case -> production environment (better to use StructType)

# COMMAND ----------

circuits_schema = "circuitId INT, circuitRef STRING, name STRING, location STRING, country STRING, lat DOUBLE, lng DOUBLE, alt DOUBLE, url STRING"

# COMMAND ----------

circuits_df = spark.read \
    .option("header", True) \
    .schema(circuits_schema) \
    .csv(f"{raw_folder_path}/circuits.csv")

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ### Read the CSV file using the spark dataframe reader and StructType
# MAGIC - define fields ourselves
# MAGIC - use case -> production environment

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DoubleType

circuits_schema = StructType(fields=[
    StructField("circuitId", IntegerType(), False),
    StructField("circuitRef", StringType(), True),
    StructField("name", StringType(), True),
    StructField("location", StringType(), True),
    StructField("country", StringType(), True),
    StructField("lat", DoubleType(), True),
    StructField("lng", DoubleType(), True),
    StructField("alt", IntegerType(), True),
    StructField("url", StringType(), True)
])

# COMMAND ----------

circuits_df = spark.read \
    .option("header", True) \
    .schema(circuits_schema) \
    .csv(f"{raw_folder_path}/circuits.csv")

# COMMAND ----------

display(circuits_df)

# COMMAND ----------

circuits_df.printSchema()

# COMMAND ----------

circuits_df.describe().show()

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC # Select only required fields from dataframe
# MAGIC ### Below there are 4 methods of selecting fields from dataframe
# MAGIC - 1st -> only select a fields
# MAGIC - 2nd, 3rd, 4th -> you have posibility to use function based on columns for example col("country").alias("race_country")

# COMMAND ----------

circuits_selected_df = circuits_df.select("circuitId", "circuitRef", "name", "location", "country", "lat", "lng", "alt")

# COMMAND ----------

circuits_selected_df = circuits_df.select(circuits_df.circuitId, circuits_df.circuitRef, circuits_df.name, 
                                          circuits_df.location, circuits_df.country, circuits_df.lat, 
                                          circuits_df.lng, circuits_df.alt)

# COMMAND ----------

circuits_selected_df = circuits_df.select(circuits_df["circuitId"], circuits_df["circuitRef"], circuits_df["name"], 
                                          circuits_df["location"], circuits_df["country"], circuits_df["lat"], 
                                          circuits_df["lng"], circuits_df["alt"])

# COMMAND ----------

from pyspark.sql.functions import col

circuits_selected_df = circuits_df.select(col("circuitId"), col("circuitRef"), col("name"), col("location"),
                                          col("country"), col("lat"), col("lng"), col("alt"))

# COMMAND ----------

display(circuits_selected_df)

# COMMAND ----------

# MAGIC %md
# MAGIC # Rename the columns as required in the dataframe

# COMMAND ----------

from pyspark.sql.functions import lit

circuits_renamed_df = circuits_selected_df.withColumnRenamed("circuitId", "circuit_id") \
                                            .withColumnRenamed("circuitRef", "circuit_ref") \
                                            .withColumnRenamed("lat", "latitude") \
                                            .withColumnRenamed("lng", "longtitude") \
                                            .withColumnRenamed("alt", "altitude") \
                                            .withColumn("data_source", lit(data_source))


# COMMAND ----------

display(circuits_renamed_df)

# COMMAND ----------

# MAGIC %md
# MAGIC # Add ingestion date to the dataframe
# MAGIC - posibility to add a column with generic function -> .withColumn("ingestion_date", current_timestamp())
# MAGIC - posibility to add static string value for all rows -> .withColumn("env", lit("Production"))

# COMMAND ----------

from pyspark.sql.functions import current_timestamp, lit

circuits_final_df = circuits_renamed_df.withColumn("ingestion_date", current_timestamp()) \
                                        .withColumn("env", lit("Production"))


# COMMAND ----------

circuits_final_df = add_ingestion_date(circuits_renamed_df)

# COMMAND ----------

display(circuits_final_df)

# COMMAND ----------

# MAGIC %md
# MAGIC # Write data to DataLake as Parquet

# COMMAND ----------

circuits_final_df.write.mode("overwrite") \
                        .parquet(f"{processed_folder_path}/circuits")

# COMMAND ----------

dbutils.fs.ls(f"{processed_folder_path}/circuits")

# COMMAND ----------

display(spark.read.parquet(f"{processed_folder_path}/circuits"))

# COMMAND ----------

# MAGIC %md
# MAGIC # Returns the value 'Success' to the place where notebook is invoked

# COMMAND ----------

dbutils.notebook.exit("Success")
