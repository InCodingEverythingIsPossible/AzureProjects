-- Databricks notebook source
-- MAGIC %md
-- MAGIC # Create Bronze Table -> external table
-- MAGIC - files: drivers.json, results.json
-- MAGIC - Bronze folder path -> abfss://bronze@databrickscourse.dfs.core.windows.net/

-- COMMAND ----------

DROP TABLE IF EXISTS formula1_dev.bronze.drivers;

CREATE TABLE IF NOT EXISTS formula1_dev.bronze.drivers
(
  driverId INT,
  driverRef STRING,
  number INT,
  code STRING,
  name STRUCT<forename: STRING, surname: STRING>,
  dob DATE,
  nationality STRING,
  url STRING
)
USING JSON
OPTIONS (path "abfss://bronze@databrickscourse.dfs.core.windows.net/drivers.json");

-- COMMAND ----------

DROP TABLE IF EXISTS formula1_dev.bronze.results;

CREATE TABLE IF NOT EXISTS formula1_dev.bronze.results
(
  constructorId INT,
  driverId INT,
  fastestLap STRING,
  fastestLapSpeed STRING,
  fastestLapTime STRING,
  grid INT,
  laps INT,
  milliseconds STRING,
  number STRING,
  points INT,
  position STRING,
  positionOrder INT,
  positionText STRING,
  raceId INT,
  rank STRING,
  resultId INT,
  statusId INT,
  time STRING
)
USING JSON
OPTIONS (path "abfss://bronze@databrickscourse.dfs.core.windows.net/results.json");
