-- Databricks notebook source
-- MAGIC %md
-- MAGIC # Create database for presentation data

-- COMMAND ----------

CREATE DATABASE IF NOT EXISTS f1_presentation
LOCATION "/mnt/formula1pw/gold"
