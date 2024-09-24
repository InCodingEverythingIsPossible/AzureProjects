-- Databricks notebook source
-- MAGIC %md
-- MAGIC # Looking information across all catalogs

-- COMMAND ----------

SELECT * 
FROM system.information_schema.tables
WHERE table_name = 'results';

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #Looking for information within one specific catalog

-- COMMAND ----------

SELECT * 
FROM formula1_dev.information_schema.tables
WHERE table_name = 'results';
