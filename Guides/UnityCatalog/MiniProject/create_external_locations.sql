-- Databricks notebook source
-- MAGIC %md
-- MAGIC # Create the external locations required for this project
-- MAGIC - external location for Bronze layer
-- MAGIC - external location for Silver layer
-- MAGIC - external location for Gold layer

-- COMMAND ----------

CREATE EXTERNAL LOCATION databrickscourse_bronze
URL 'abfss://bronze@databrickscourse.dfs.core.windows.net/'
WITH (STORAGE CREDENTIAL `databrickscourse-ext-storage-credential`);

-- COMMAND ----------

DESC EXTERNAL LOCATION databrickscourse_bronze;

-- COMMAND ----------

-- MAGIC %python
-- MAGIC dbutils.fs.ls("abfss://bronze@databrickscourse.dfs.core.windows.net/")
-- MAGIC

-- COMMAND ----------

CREATE EXTERNAL LOCATION databrickscourse_silver
URL 'abfss://silver@databrickscourse.dfs.core.windows.net/'
WITH (STORAGE CREDENTIAL `databrickscourse-ext-storage-credential`);

-- COMMAND ----------

CREATE EXTERNAL LOCATION databrickscourse_gold
URL 'abfss://gold@databrickscourse.dfs.core.windows.net/'
WITH (STORAGE CREDENTIAL `databrickscourse-ext-storage-credential`);
