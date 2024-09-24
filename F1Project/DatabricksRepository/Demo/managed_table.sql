-- Databricks notebook source
-- MAGIC %md
-- MAGIC # Managed table
-- MAGIC - Create managed table using Python
-- MAGIC - Create managed table using SQL
-- MAGIC - Dropping a managed table -> drop data and metadata of table

-- COMMAND ----------

-- MAGIC %md
-- MAGIC # Importing configuration notebook for generic cases

-- COMMAND ----------

-- MAGIC %run "../Includes/configuration"

-- COMMAND ----------

-- MAGIC %md
-- MAGIC # Create managed table using Python

-- COMMAND ----------

-- MAGIC %python
-- MAGIC race_results_df = spark.read.parquet(f"{presentation_folder_path}/race_results")

-- COMMAND ----------

-- MAGIC %python
-- MAGIC race_results_df.write.format("parquet").saveAsTable("demo.race_results_python")

-- COMMAND ----------

USE demo;
SHOW TABLES;

-- COMMAND ----------

DESCRIBE race_results_python

-- COMMAND ----------

DESCRIBE EXTENDED race_results_python

-- COMMAND ----------

SELECT *
FROM demo.race_results_python
WHERE race_year = 2020

-- COMMAND ----------

-- MAGIC %md
-- MAGIC # Create managed table using SQL

-- COMMAND ----------

CREATE TABLE demo.race_results_sql
AS
SELECT *
FROM demo.race_results_python
WHERE race_year = 2020;

-- COMMAND ----------

DESCRIBE EXTENDED demo.race_results_sql

-- COMMAND ----------

DROP TABLE demo.race_results_sql

-- COMMAND ----------

-- MAGIC %md
-- MAGIC
