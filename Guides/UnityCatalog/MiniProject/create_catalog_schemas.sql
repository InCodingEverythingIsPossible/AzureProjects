-- Databricks notebook source
-- MAGIC %md
-- MAGIC # Hint for specifying managed location
-- MAGIC
-- MAGIC ### If managed location will be specified on lower level namespace it will base on that
-- MAGIC - schemas managed location (1st)
-- MAGIC - catalog managed location (2nd)
-- MAGIC - metastore managed location (3rd)

-- COMMAND ----------

-- MAGIC %md
-- MAGIC # Create Catalogs and Schemas required for the project
-- MAGIC - Catalog - formula1dev (without specified managed location)
-- MAGIC - Schemas - bronze, silver, gold (with specified managed location)

-- COMMAND ----------

CREATE CATALOG IF NOT EXISTS formula1_dev;

-- COMMAND ----------

USE CATALOG formula1_dev;

-- COMMAND ----------

CREATE SCHEMA IF NOT EXISTS bronze
MANAGED LOCATION "abfss://bronze@databrickscourse.dfs.core.windows.net/"

-- COMMAND ----------

CREATE SCHEMA IF NOT EXISTS silver
MANAGED LOCATION "abfss://silver@databrickscourse.dfs.core.windows.net/"

-- COMMAND ----------

CREATE SCHEMA IF NOT EXISTS gold
MANAGED LOCATION "abfss://gold@databrickscourse.dfs.core.windows.net/"

-- COMMAND ----------

SHOW SCHEMAS;
