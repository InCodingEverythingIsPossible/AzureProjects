-- Databricks notebook source
-- MAGIC %md
-- MAGIC # SQL joins introduction

-- COMMAND ----------

DESC f1_presentation.driver_standings

-- COMMAND ----------

CREATE OR REPLACE TEMP VIEW v_driver_standings_2018
AS
SELECT race_year, driver_name, team, total_points, wins, rank
FROM f1_presentation.driver_standings
WHERE race_year = 2018;

-- COMMAND ----------

CREATE OR REPLACE TEMP VIEW v_driver_standings_2020
AS
SELECT race_year, driver_name, team, total_points, wins, rank
FROM f1_presentation.driver_standings
WHERE race_year = 2020;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC # Inner Join

-- COMMAND ----------

SELECT *
FROM v_driver_standings_2018 as d_2018
JOIN v_driver_standings_2020 as d_2020
    ON d_2018.driver_name = d_2020.driver_name;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC # Left Join

-- COMMAND ----------

SELECT *
FROM v_driver_standings_2018 as d_2018
LEFT JOIN v_driver_standings_2020 as d_2020
    ON d_2018.driver_name = d_2020.driver_name;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC # Right Join

-- COMMAND ----------

SELECT *
FROM v_driver_standings_2018 as d_2018
RIGHT JOIN v_driver_standings_2020 as d_2020
    ON d_2018.driver_name = d_2020.driver_name;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC # Full Join

-- COMMAND ----------

SELECT *
FROM v_driver_standings_2018 as d_2018
FULL JOIN v_driver_standings_2020 as d_2020
    ON d_2018.driver_name = d_2020.driver_name;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC # Semi Join

-- COMMAND ----------

SELECT *
FROM v_driver_standings_2018 as d_2018
SEMI JOIN v_driver_standings_2020 as d_2020
    ON d_2018.driver_name = d_2020.driver_name;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC # Anti Join

-- COMMAND ----------

SELECT *
FROM v_driver_standings_2018 as d_2018
ANTI JOIN v_driver_standings_2020 as d_2020
    ON d_2018.driver_name = d_2020.driver_name;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC # Cross Join

-- COMMAND ----------

SELECT *
FROM v_driver_standings_2018 as d_2018
CROSS JOIN v_driver_standings_2020 as d_2020;
