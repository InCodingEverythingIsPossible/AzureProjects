# Databricks notebook source
#The function is invoked in all ../Ingest/ingest_* notebooks to add ingestion_date

from pyspark.sql import DataFrame
from pyspark.sql.functions import current_timestamp

def add_ingestion_date(input_df: DataFrame):
    output_df = input_df.withColumn("ingestion_date", current_timestamp())
    return output_df

# COMMAND ----------

#The function deals with loading data incrementally on parquet files

def incrementalLoad(input_df: DataFrame, databaseName: str, tableName: str, partitionField: str):

    #In this mode, Spark deletes and overwrites all partitions that are specified by the input
    spark.conf.set("spark.sql.sources.partitionOverwriteMode", "dynamic")

    #Overwrite partition specified by the input (last column in dataframe)
    if (spark._jsparkSession.catalog().tableExists(f"{databaseName}.{tableName}")):
        input_df.write.mode("overwrite") \
                      .insertInto(f"{databaseName}.{tableName}")

    #Create table and insert first data with partitioning by specified field
    else:
        input_df.write.mode("overwrite") \
                      .partitionBy(partitionField) \
                      .format("parquet") \
                      .saveAsTable(f"{databaseName}.{tableName}")

# COMMAND ----------

#The function deals with placing the partition field in the last place in the dataframe

def sortForIncrementalLoad(input_df: DataFrame, partitionField: str):

    #Order of fields in the dataframe
    columns = [col for col in input_df.columns if col != partitionField] + [partitionField]

    #Return sorted the dataframe
    return input_df.select(columns)
     

# COMMAND ----------

#The function deals with loading data incrementally on delta files

from delta.tables import DeltaTable

def incrementalLoadDelta(input_df: DataFrame, databaseName: str, tableName: str, folderPath: str, partitionField: str, mergeCondition: str):

    #In this mode, Spark force to use a column to find a correct partition of the file. \
    #For example (target.race_id = src.race_id). It helps with performance.
    spark.conf.set("spark.databricks.optimizer.dynamicPartitionPruning", "true")

    #If the table exists, merge the data based on mergeCondition. 
    if(spark._jsparkSession.catalog().tableExists(f"{databaseName}.{tableName}")):
        
        #Save existed delta table to variable for merge process
        deltaTable = DeltaTable.forPath(spark, f"{folderPath}/{tableName}")
        
        #If the merge key is the same, update the entire row 
        #If the merge key is not the same, insert the entire row
        deltaTable.alias("target").merge( input_df.alias("source"), mergeCondition) \
                                  .whenMatchedUpdateAll() \
                                  .whenNotMatchedInsertAll() \
                                  .execute()
    #Otherwise create table and insert first data with partitioning by specified field
    else:
        input_df.write.mode("overwrite") \
                      .partitionBy(partitionField) \
                      .format("delta") \
                      .saveAsTable(f"{databaseName}.{tableName}")
                                            
    


