-- Databricks notebook source
-- MAGIC %md
-- MAGIC # Views using tables
-- MAGIC - Create Temp View -> only available in the notebook where the view was created and in current session of cluster
-- MAGIC - Create Global Temp View -> only available in current session of cluster from any notebook which use same cluster
-- MAGIC - Create Permanent View -> permanent view

-- COMMAND ----------

-- MAGIC %md
-- MAGIC # Create Temp View using table

-- COMMAND ----------

CREATE OR REPLACE TEMP VIEW v_race_results
AS
SELECT *
FROM demo.race_results_python
WHERE race_year = 2018;

-- COMMAND ----------

SELECT * 
FROM v_race_results

-- COMMAND ----------

-- MAGIC %md
-- MAGIC # Create Global Temp View using table

-- COMMAND ----------

CREATE OR REPLACE GLOBAL TEMP VIEW gv_race_results
AS
SELECT *
FROM demo.race_results_python
WHERE race_year = 2018;

-- COMMAND ----------

SELECT * 
FROM global_temp.gv_race_results

-- COMMAND ----------

-- MAGIC %md
-- MAGIC # Create Permanent View using table

-- COMMAND ----------

CREATE OR REPLACE VIEW demo.pv_race_results
AS
SELECT *
FROM demo.race_results_python
WHERE race_year = 2018;

-- COMMAND ----------

SELECT * 
FROM demo.pv_race_results;
