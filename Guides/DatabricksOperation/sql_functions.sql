-- Databricks notebook source
-- MAGIC %md
-- MAGIC # SQL functions introduction

-- COMMAND ----------

-- MAGIC %md
-- MAGIC # Scalar functions SQL

-- COMMAND ----------

SELECT *, CONCAT(driver_ref, '-', code) AS new_driver_ref
FROM f1_processed.drivers;

-- COMMAND ----------

SELECT *, SPLIT(name, ' ')[0] forename, SPLIT(name, ' ')[1] surname
FROM f1_processed.drivers;

-- COMMAND ----------

SELECT *, current_timestamp
FROM f1_processed.drivers;

-- COMMAND ----------

SELECT *, date_format(dob, 'dd-MM-yyyy')
FROM f1_processed.drivers;

-- COMMAND ----------

SELECT *, date_add(dob, 1)
FROM f1_processed.drivers;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC # Aggregate functions SQL

-- COMMAND ----------

SELECT COUNT(*)
FROM f1_processed.drivers;

-- COMMAND ----------

SELECT MAX(dob)
FROM f1_processed.drivers;

-- COMMAND ----------

SELECT COUNT(*)
FROM f1_processed.drivers
WHERE nationality = 'British';

-- COMMAND ----------

SELECT nationality, COUNT(*)
FROM f1_processed.drivers
GROUP BY nationality
ORDER BY nationality;

-- COMMAND ----------

SELECT nationality, COUNT(*)
FROM f1_processed.drivers
GROUP BY nationality
HAVING COUNT(*) > 100
ORDER BY nationality;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC # Window functions SQL

-- COMMAND ----------

SELECT nationality, name, dob, RANK() OVER(PARTITION BY nationality ORDER BY dob) as age_rank
FROM f1_processed.drivers;

-- COMMAND ----------


