# Databricks notebook source
# Mount configuration for mount point with all files

raw_folder_path = "/mnt/formula1pw/bronze"
processed_folder_path = "/mnt/formula1pw/silver"
presentation_folder_path = "/mnt/formula1pw/gold"

# COMMAND ----------

#ABFSS configuration for datalake with all files

#raw_folder_path = "abfss://bronze@formula1pw.dfs.core.windows.net"
#processed_folder_path = "abfss://silver@formula1pw.dfs.core.windows.net"
#presentation_folder_path = "abfss://gold@formula1pw.dfs.core.windows.net"
