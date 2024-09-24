-- Databricks notebook source
-- MAGIC %md
-- MAGIC # Query data via unity catalog using 3 level namespace
-- MAGIC
-- MAGIC
-- MAGIC ### 3 level namespace
-- MAGIC - catalog -> dev, test, prod
-- MAGIC - schema (database) -> bronze, silver, gold
-- MAGIC - tables -> table name

-- COMMAND ----------

SELECT * 
FROM demo_catalog.demo_schema.circuits;

-- COMMAND ----------

SELECT current_catalog()

-- COMMAND ----------

SHOW CATALOGS;

-- COMMAND ----------

USE CATALOG demo_catalog;

-- COMMAND ----------

SELECT current_catalog();

-- COMMAND ----------

SELECT current_schema();

-- COMMAND ----------

SHOW SCHEMAS;

-- COMMAND ----------

USE SCHEMA demo_schema;

-- COMMAND ----------

SELECT * FROM circuits;

-- COMMAND ----------

SHOW tables;

-- COMMAND ----------

-- MAGIC %python
-- MAGIC display(spark.sql('SHOW TABLES'))

-- COMMAND ----------

-- MAGIC %python
-- MAGIC df = spark.table('demo_catalog.demo_schema.circuits')

-- COMMAND ----------

-- MAGIC %python
-- MAGIC display(df)
