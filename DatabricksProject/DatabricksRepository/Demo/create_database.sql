-- Databricks notebook source
-- MAGIC %md
-- MAGIC # Create database
-- MAGIC - Create Database demo
-- MAGIC - Catalog tab in the databricks UI
-- MAGIC - SHOW command -> show all databases
-- MAGIC - DESCRIBE command -> describing an object
-- MAGIC - Find the current databse

-- COMMAND ----------

CREATE DATABASE demo;

-- COMMAND ----------

CREATE DATABASE IF NOT EXISTS demo;

-- COMMAND ----------

SHOW databases;

-- COMMAND ----------

SHOW TABLES

-- COMMAND ----------

SHOW TABLES IN demo

-- COMMAND ----------

DESCRIBE DATABASE demo;

-- COMMAND ----------

DESCRIBE DATABASE EXTENDED demo;

-- COMMAND ----------

SELECT current_database()

-- COMMAND ----------

USE demo;

-- COMMAND ----------

SELECT current_database()
