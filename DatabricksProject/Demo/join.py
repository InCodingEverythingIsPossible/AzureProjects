# Databricks notebook source
# MAGIC %md
# MAGIC # Spark Join Transformation

# COMMAND ----------

# MAGIC %md
# MAGIC # Importing configuration notebooks for generic cases

# COMMAND ----------

# MAGIC %run "../Includes/configuration"

# COMMAND ----------

# MAGIC %md
# MAGIC # Read Parquet file using spark dataframe reader
# MAGIC
# MAGIC ## Best practises in join operation
# MAGIC - rename columns to avoid duplicate names
# MAGIC - filter the data to use only what we need

# COMMAND ----------

circuits_df = spark.read.parquet(f"{processed_folder_path}/circuits") \
                        .filter("circuit_id < 70") \
                        .withColumnRenamed("name","circuit_name")

# COMMAND ----------

races_df = spark.read.parquet(f"{processed_folder_path}/races") \
                        .filter("race_year = 2019") \
                        .withColumnRenamed("name","race_name")

# COMMAND ----------

display(races_df)

# COMMAND ----------

# MAGIC %md
# MAGIC # Inner Join race_df with circuit_df 
# MAGIC
# MAGIC ## Using join structure 
# MAGIC - 1st -> doesn't provide which join we want to use (default value "inner")
# MAGIC - 2nd -> provide that we are using inner join 
# MAGIC - 3rd -> in second parameter we use name of column which is key to join in both dataframe
# MAGIC - if more than one key use [df.name == df3.name, df.age == df3.age] or ['name', 'age']
# MAGIC
# MAGIC
# MAGIC ## After join select necessary fields for further development

# COMMAND ----------

race_circuits_df = circuits_df.join(races_df, circuits_df.circuit_id == races_df.circuit_id) \
                                .select(circuits_df.circuit_name, circuits_df.location, circuits_df.country, races_df.race_name, races_df.round)

# COMMAND ----------

race_circuits_df = circuits_df.join(races_df, circuits_df.circuit_id == races_df.circuit_id, "inner") \
                                .select(circuits_df.circuit_name, circuits_df.location, circuits_df.country, races_df.race_name, races_df.round)

# COMMAND ----------

race_circuits_df = circuits_df.join(races_df, "circuit_id", "inner") \
                                .select(circuits_df.circuit_name, circuits_df.location, circuits_df.country, races_df.race_name, races_df.round)

# COMMAND ----------

display(race_circuits_df)

# COMMAND ----------

# MAGIC %md
# MAGIC # Left Join race_df with circuit_df 

# COMMAND ----------

race_circuits_df = circuits_df.join(races_df, circuits_df.circuit_id == races_df.circuit_id, "left") \
                                .select(circuits_df.circuit_name, circuits_df.location, circuits_df.country, races_df.race_name, races_df.round)

# COMMAND ----------

display(race_circuits_df)

# COMMAND ----------

# MAGIC %md
# MAGIC # Right Join race_df with circuit_df 

# COMMAND ----------

race_circuits_df = circuits_df.join(races_df, circuits_df.circuit_id == races_df.circuit_id, "right") \
                                .select(circuits_df.circuit_name, circuits_df.location, circuits_df.country, races_df.race_name, races_df.round)

# COMMAND ----------

display(race_circuits_df)

# COMMAND ----------

# MAGIC %md
# MAGIC # Full Join race_df with circuit_df 

# COMMAND ----------

race_circuits_df = circuits_df.join(races_df, circuits_df.circuit_id == races_df.circuit_id, "full") \
                                .select(circuits_df.circuit_name, circuits_df.location, circuits_df.country, races_df.race_name, races_df.round)

# COMMAND ----------

display(race_circuits_df)

# COMMAND ----------

# MAGIC %md
# MAGIC # Semi Join race_df with circuit_df 
# MAGIC
# MAGIC - Working in the same way like inner join but in the output available are only columns from the left dataframe

# COMMAND ----------

race_circuits_df = circuits_df.join(races_df, circuits_df.circuit_id == races_df.circuit_id, "semi")

# COMMAND ----------

display(race_circuits_df)

# COMMAND ----------

# MAGIC %md
# MAGIC # Anti Join race_df with circuit_df 
# MAGIC
# MAGIC - Working in the opossite way to inner join but in the output available are only columns from the left dataframe
# MAGIC - Returns records from the left dataframe which doesn't match to records in the right dataframe 

# COMMAND ----------

race_circuits_df = circuits_df.join(races_df, circuits_df.circuit_id == races_df.circuit_id, "anti")

# COMMAND ----------

display(race_circuits_df)

# COMMAND ----------

# MAGIC %md
# MAGIC # Cross Join race_df with circuit_df 
# MAGIC
# MAGIC - each record from the left dataframe is join to the right dataframe
# MAGIC

# COMMAND ----------

race_circuits_df = circuits_df.crossJoin(races_df)

# COMMAND ----------

display(race_circuits_df)
