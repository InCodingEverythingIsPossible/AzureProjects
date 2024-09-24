-- Databricks notebook source
-- MAGIC %md
-- MAGIC # Create database for raw data

-- COMMAND ----------

CREATE DATABASE IF NOT EXISTS f1_raw;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC # Create circuits table
-- MAGIC - Single CSV file

-- COMMAND ----------

DROP TABLE IF EXISTS f1_raw.circuits;

CREATE TABLE IF NOT EXISTS f1_raw.circuits(
  circuitId INT, 
  circuitRef STRING, 
  name STRING, 
  location STRING, 
  country STRING, 
  lat DOUBLE, 
  lng DOUBLE, 
  alt DOUBLE, 
  url STRING
)
USING CSV
OPTIONS (path "/mnt/formula1pw/bronze/circuits.csv", header True);

-- COMMAND ----------

SELECT *
FROM f1_raw.circuits

-- COMMAND ----------

-- MAGIC %md
-- MAGIC # Create races table
-- MAGIC - Single CSV file

-- COMMAND ----------

DROP TABLE IF EXISTS f1_raw.races;

CREATE TABLE IF NOT EXISTS f1_raw.races(
  raceId INT, 
  year STRING, 
  round STRING, 
  circuitId STRING, 
  name STRING, 
  date DATE, 
  time STRING, 
  url STRING
)
USING CSV
OPTIONS (path "/mnt/formula1pw/bronze/races.csv", header True);

-- COMMAND ----------

SELECT * 
FROM f1_raw.races

-- COMMAND ----------

-- MAGIC %md
-- MAGIC # Create constructors table
-- MAGIC - Single Line JSON
-- MAGIC - Simple structure

-- COMMAND ----------

DROP TABLE IF EXISTS f1_raw.constructors;

CREATE TABLE IF NOT EXISTS f1_raw.constructors(
  constructorId INT, 
  constructorRef STRING, 
  name STRING, 
  nationality STRING, 
  url STRING
)
USING JSON
OPTIONS (path "/mnt/formula1pw/bronze/constructors.json");

-- COMMAND ----------

SELECT *
FROM f1_raw.constructors

-- COMMAND ----------

-- MAGIC %md
-- MAGIC # Create drivers table
-- MAGIC - Single Line JSON
-- MAGIC - Complex structure

-- COMMAND ----------

DROP TABLE IF EXISTS f1_raw.drivers;

CREATE TABLE IF NOT EXISTS f1_raw.drivers(
  driverId INT,
  driverRef STRING,
  number INT,
  code STRING,
  name STRUCT <forename: STRING, surname: STRING>,
  dob DATE,
  nationality STRING,
  url STRING
)
USING JSON
OPTIONS(path "/mnt/formula1pw/bronze/drivers.json");

-- COMMAND ----------

SELECT *
FROM f1_raw.drivers;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC # Create results table
-- MAGIC - Single Line JSON
-- MAGIC - Simple structure

-- COMMAND ----------

DROP TABLE IF EXISTS f1_raw.results;

CREATE TABLE IF NOT EXISTS f1_raw.results(
  constructorId INT,
  driverId INT,
  fastestLap STRING,
  fastestLapSpeed STRING,
  fastestLapTime STRING,
  grid INT,
  laps INT,
  milliseconds STRING,
  number STRING,
  points DOUBLE,
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
OPTIONS(path "/mnt/formula1pw/bronze/results.json");

-- COMMAND ----------

SELECT *
FROM f1_raw.results

-- COMMAND ----------

-- MAGIC %md
-- MAGIC # Create pit stops table
-- MAGIC - Multi Line JSON
-- MAGIC - Simple structure

-- COMMAND ----------

DROP TABLE IF EXISTS f1_raw.pit_stops;

CREATE TABLE IF NOT EXISTS f1_raw.pit_stops(
  raceId INT,
  driverId INT,
  stop STRING,
  lap INT,
  time STRING,
  duration STRING,
  milliseconds INT
)
USING JSON
OPTIONS(path "/mnt/formula1pw/bronze/pit_stops.json", multiline True);

-- COMMAND ----------

SELECT *
FROM f1_raw.pit_stops;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC # Create Lap Times Table
-- MAGIC - CSV file
-- MAGIC - Multiple files

-- COMMAND ----------

DROP TABLE IF EXISTS f1_raw.lap_times;

CREATE TABLE IF NOT EXISTS f1_raw.lap_times(
  raceId INT,
  driverId INT,
  lap INT,
  position INT,
  time STRING,
  milliseconds INT
)
USING CSV
OPTIONS (path "/mnt/formula1pw/bronze/lap_times");

-- COMMAND ----------

SELECT * 
FROM f1_raw.lap_times

-- COMMAND ----------

-- MAGIC %md
-- MAGIC # Create Qualifying Table
-- MAGIC - JSON file
-- MAGIC - MultiLine JSON
-- MAGIC - Multiple files

-- COMMAND ----------

DROP TABLE IF EXISTS f1_raw.qualifying;

CREATE TABLE IF NOT EXISTS f1_raw.qualifying(
  qualifyId INT,
  raceId INT,
  driverId INT,
  constructorId INT,
  number INT,
  position INT,
  q1 STRING,
  q2 STRING,
  q3 STRING
)
USING JSON
OPTIONS(path "/mnt/formula1pw/bronze/qualifying", multiline True);

-- COMMAND ----------

SELECT *
FROM f1_raw.qualifying
