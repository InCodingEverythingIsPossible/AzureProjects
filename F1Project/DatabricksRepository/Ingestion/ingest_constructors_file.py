# Databricks notebook source
# MAGIC %md
# MAGIC # Ingest constructors.json file

# COMMAND ----------

# MAGIC %md
# MAGIC # Passing parameters to the notebook

# COMMAND ----------

dbutils.widgets.text("data_source", "")
data_source = dbutils.widgets.get("data_source")

# COMMAND ----------

dbutils.widgets.text("file_date", "2021-03-21")
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
# MAGIC # Read the JSON (single line json file) using the spark dataframe reader with DDL schema of file

# COMMAND ----------

constructors_schema = "constructorId INT, constructorRef STRING, name STRING, nationality STRING, url STRING"

# COMMAND ----------

constructors_df = spark.read \
    .schema(constructors_schema) \
    .json(f"{raw_folder_path}/{file_date}/constructors.json")


# COMMAND ----------

display(constructors_df)

# COMMAND ----------

# MAGIC %md
# MAGIC # Drop unwanted columns from the dataframe
# MAGIC - 1st, 2nd -> use when we use only one dataframe
# MAGIC - 3rd -> use when we use more than one dataframe

# COMMAND ----------

constructors_dropped_df = constructors_df.drop('url')

# COMMAND ----------

from pyspark.sql.functions import col

constructors_dropped_df = constructors_df.drop(col('url'))

# COMMAND ----------

constructors_dropped_df = constructors_df.drop(constructors_df['url'])

# COMMAND ----------

# MAGIC %md
# MAGIC # Rename columns and add ingestion date

# COMMAND ----------

constructors_ingestion_date_df = add_ingestion_date(constructors_dropped_df)

# COMMAND ----------

from pyspark.sql.functions import lit

constructors_final_df = constructors_ingestion_date_df.withColumnRenamed("constructorId", "constructor_id") \
                                                .withColumnRenamed("constructorRef", "constructor_ref") \
                                                .withColumn("data_source", lit(data_source)) \
                                                .withColumn("file_date", lit(file_date))
        

# COMMAND ----------

display(constructors_final_df)

# COMMAND ----------

# MAGIC %md
# MAGIC # Write dataframe to the container in Parquet format

# COMMAND ----------

# constructors_final_df.write.mode("overwrite") \
#                        .format("parquet") \
#                        .saveAsTable("f1_processed.constructors")

# COMMAND ----------

# display(spark.read.parquet(f"{processed_folder_path}/constructors"))

# COMMAND ----------

# MAGIC %md
# MAGIC # Write dataframe to the container in Delta format

# COMMAND ----------

constructors_final_df.write.mode("overwrite") \
                       .format("delta") \
                       .saveAsTable("f1_processed.constructors")

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * 
# MAGIC FROM f1_processed.constructors;

# COMMAND ----------

# MAGIC %md
# MAGIC # Returns the value 'Success' to the place where notebook is invoked

# COMMAND ----------

dbutils.notebook.exit("Success")
