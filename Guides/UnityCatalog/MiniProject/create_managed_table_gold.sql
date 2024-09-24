-- Databricks notebook source
-- MAGIC %md
-- MAGIC # Create Gold Table -> managed table
-- MAGIC - join drivers and results to identify the number of wins per drivers

-- COMMAND ----------

DROP TABLE IF EXISTS formula1_dev.gold.driver_wins;

CREATE TABLE formula1_dev.gold.drivers_wins;
AS
SELECT drivers.name,
       COUNT(1) AS number_of_wins,
FROM formula1_dev.silver.drivers drivers
JOIN formula1_dev.silver.results results
    ON drivers.driverId = results.driverId
WHERE results.position = 1
GROUP BY drivers.name;

-- COMMAND ----------

SELECT *
FROM formula1_dev.gold.drivers_wins
ORDER BY number_of_wins DESC;
