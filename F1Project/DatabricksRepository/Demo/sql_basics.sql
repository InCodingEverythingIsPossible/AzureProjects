-- Databricks notebook source
-- MAGIC %md
-- MAGIC # SQL basics

-- COMMAND ----------

SHOW DATABASES;

-- COMMAND ----------

SELECT CURRENT_DATABASE()

-- COMMAND ----------

USE f1_processed;

-- COMMAND ----------

SHOW TABLES;

-- COMMAND ----------

SELECT *
FROM f1_processed.drivers
LIMIT 10;

-- COMMAND ----------

DESC f1_processed.drivers;

-- COMMAND ----------

SELECT *
FROM f1_processed.drivers
WHERE nationality = 'British'
        AND dob >= '1990-01-01';

-- COMMAND ----------

SELECT name, dob AS date_of_birth
FROM f1_processed.drivers
WHERE nationality = 'British'
        AND dob >= '1990-01-01';

-- COMMAND ----------

SELECT name, dob AS date_of_birth
FROM f1_processed.drivers
WHERE nationality = 'British'
        AND dob >= '1990-01-01'
ORDER BY date_of_birth DESC;

-- COMMAND ----------

SELECT *
FROM f1_processed.drivers
ORDER BY nationality, 
         dob DESC;

-- COMMAND ----------

SELECT name, nationality, dob AS date_of_birth
FROM f1_processed.drivers
WHERE (nationality = 'British' AND dob >= '1990-01-01')
      OR
      (nationality = 'Indian')
ORDER BY date_of_birth DESC;

-- COMMAND ----------


