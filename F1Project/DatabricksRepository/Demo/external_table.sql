-- Databricks notebook source
-- MAGIC %md
-- MAGIC # External table
-- MAGIC - Create external table using Python
-- MAGIC - Create external table using SQL
-- MAGIC - Dropping a external table -> drop only metadata

-- COMMAND ----------

-- MAGIC %md
-- MAGIC # Importing configuration notebook for generic cases

-- COMMAND ----------

-- MAGIC %run "../Includes/configuration"

-- COMMAND ----------

-- MAGIC %md
-- MAGIC # Create external table using Python

-- COMMAND ----------

-- MAGIC %python
-- MAGIC race_results_df = spark.read.parquet(f"{presentation_folder_path}/race_results")

-- COMMAND ----------

-- MAGIC %python
-- MAGIC race_results_df.write.format("parquet").option("path",f"{presentation_folder_path}/race_results_ext").saveAsTable("demo.race_results_ext_py")

-- COMMAND ----------

USE demo;
SHOW TABLES;

-- COMMAND ----------

DESCRIBE race_results_ext_py

-- COMMAND ----------

DESCRIBE EXTENDED race_results_ext_py

-- COMMAND ----------

SELECT *
FROM demo.race_results_ext_py
WHERE race_year = 2020

-- COMMAND ----------

-- MAGIC %md
-- MAGIC # Create managed table using SQL

-- COMMAND ----------

CREATE TABLE demo.race_results_ext_sql
  (
    race_year INT,
    race_name STRING,
    race_date TIMESTAMP,
    circuit_location STRING,
    driver_name STRING,
    driver_number STRING,
    driver_nationality STRING,
    team STRING,
    grid INT,
    fastest_lap STRING,
    race_time STRING,
    points FLOAT,
    position STRING,
    created_date TIMESTAMP
  )
USING PARQUET
LOCATION "/mnt/formula1pw/gold/race_results_ext_sql"

-- COMMAND ----------

INSERT INTO demo.race_results_ext_sql
SELECT *
FROM demo.race_results_ext_py
WHERE race_year = 2020;

-- COMMAND ----------

DESCRIBE EXTENDED demo.race_results_ext_sql

-- COMMAND ----------

DROP TABLE demo.race_results_ext_sql

-- COMMAND ----------

-- MAGIC %md
-- MAGIC
