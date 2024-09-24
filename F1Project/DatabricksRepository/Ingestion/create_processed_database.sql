-- Databricks notebook source
-- MAGIC %md
-- MAGIC # Create database for processed data

-- COMMAND ----------

CREATE DATABASE IF NOT EXISTS f1_processed
LOCATION "/mnt/formula1pw/silver"
