# Databricks notebook source
# MAGIC %md
# MAGIC # Delta lake demo
# MAGIC - Write data to delta lake (managed table)
# MAGIC - Write data to delta lake (external table)
# MAGIC - Read data from delta lake (Table)
# MAGIC - Read data from delta lake (File)
# MAGIC - Update Delta Table
# MAGIC - Delete from Delta Table
# MAGIC - Merge Into Delta Table
# MAGIC - History & Versioning (analyze history of delta table)
# MAGIC - Time Travel (query previous versions of delta table)
# MAGIC - Vaccum (delete history of delta table default 7 days)
# MAGIC - Restore deleted data (based on previous version of delta table)
# MAGIC - Transaction Logs (deleted default after 30 days)
# MAGIC - Convert Parquet to Delta

# COMMAND ----------

# MAGIC %md
# MAGIC # Creating database demo and reading the dataframe

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE DATABASE IF NOT EXISTS f1_demo
# MAGIC LOCATION '/mnt/formula1pw/demo'

# COMMAND ----------

results_df = spark.read \
                  .option("inferSchema", True) \
                  .json("/mnt/formula1pw/bronze/2021-03-28/results.json")

# COMMAND ----------

# MAGIC %md
# MAGIC #Write data to delta lake using managed table

# COMMAND ----------

results_df.write.format("delta") \
                .mode("overwrite") \
                .saveAsTable("f1_demo.results_managed")    

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * 
# MAGIC FROM f1_demo.results_managed;

# COMMAND ----------

# MAGIC %md
# MAGIC #Write data to delta lake using external table

# COMMAND ----------

results_df.write.format("delta") \
                .mode("overwrite") \
                .save("/mnt/formula1pw/demo/results_external")    

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE f1_demo.results_external
# MAGIC USING DELTA
# MAGIC LOCATION '/mnt/formula1pw/demo/results_external'

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * 
# MAGIC FROM f1_demo.results_external;

# COMMAND ----------

# MAGIC %md
# MAGIC #Read data from delta lake using delta file

# COMMAND ----------

results_external_df = spark.read.format("delta") \
                                .load("/mnt/formula1pw/demo/results_external")

# COMMAND ----------

display(results_external_df)

# COMMAND ----------

# MAGIC %md
# MAGIC #Write data to delta lake using managed table and partition

# COMMAND ----------

results_df.write.format("delta") \
                .mode("overwrite") \
                .partitionBy("constructorId") \
                .saveAsTable("f1_demo.results_partitioned") 

# COMMAND ----------

# MAGIC %sql
# MAGIC SHOW PARTITIONS f1_demo.results_partitioned

# COMMAND ----------

# MAGIC %md
# MAGIC #Update Delta Table
# MAGIC - update using sql
# MAGIC - update using python

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT *
# MAGIC FROM f1_demo.results_managed;

# COMMAND ----------

# MAGIC %sql
# MAGIC UPDATE f1_demo.results_managed
# MAGIC SET POINTS = 11 - position
# MAGIC WHERE position <= 10;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT *
# MAGIC FROM f1_demo.results_managed;

# COMMAND ----------

from delta.tables import DeltaTable

deltaTable = DeltaTable.forPath(spark,"/mnt/formula1pw/demo/results_managed")

deltaTable.update("position <= 10", {"points": "21-position"})

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT *
# MAGIC FROM f1_demo.results_managed;

# COMMAND ----------

# MAGIC %md
# MAGIC # Delete from Delta Table
# MAGIC - delete using sql
# MAGIC - delete using python

# COMMAND ----------

# MAGIC %sql
# MAGIC DELETE FROM f1_demo.results_managed
# MAGIC WHERE position > 10;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT *
# MAGIC FROM f1_demo.results_managed;

# COMMAND ----------

from delta.tables import DeltaTable

deltaTable = DeltaTable.forPath(spark,"/mnt/formula1pw/demo/results_managed")

deltaTable.delete("points = 0")

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT *
# MAGIC FROM f1_demo.results_managed;

# COMMAND ----------

# MAGIC %md
# MAGIC #Upsert using merge
# MAGIC - merge using sql
# MAGIC - merge using python

# COMMAND ----------

drivers_day1_df = spark.read \
                       .option("inferSchema", True) \
                       .json("/mnt/formula1pw/bronze/2021-03-28/drivers.json") \
                       .filter("driverId <= 10") \
                       .select("driverId","dob","name.forename","name.surname")

# COMMAND ----------

display(drivers_day1_df)

# COMMAND ----------

drivers_day1_df.createOrReplaceTempView("drivers_day1")

# COMMAND ----------

from pyspark.sql.functions import upper

drivers_day2_df = spark.read \
                       .option("inferSchema", True) \
                       .json("/mnt/formula1pw/bronze/2021-03-28/drivers.json") \
                       .filter("driverId BETWEEN 6 and 15") \
                       .select("driverId", "dob", upper("name.forename").alias("forename"), upper("name.surname").alias("surname"))

# COMMAND ----------

display(drivers_day2_df)

# COMMAND ----------

drivers_day2_df.createOrReplaceTempView("drivers_day2")

# COMMAND ----------

from pyspark.sql.functions import upper

drivers_day3_df = spark.read \
                       .option("inferSchema", True) \
                       .json("/mnt/formula1pw/bronze/2021-03-28/drivers.json") \
                       .filter("driverId BETWEEN 1 AND 5 OR driverId BETWEEN 16 AND 20") \
                       .select("driverId", "dob", upper("name.forename").alias("forename"), upper("name.surname").alias("surname"))

# COMMAND ----------

display(drivers_day3_df)

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS f1_demo.drivers_merge (
# MAGIC   driverId INT,
# MAGIC   dob DATE,
# MAGIC   forename STRING,
# MAGIC   surname STRING,
# MAGIC   createdDate DATE,
# MAGIC   updatedDate DATE
# MAGIC )
# MAGIC USING DELTA

# COMMAND ----------

# MAGIC %sql
# MAGIC MERGE INTO f1_demo.drivers_merge
# MAGIC USING drivers_day1
# MAGIC   ON drivers_merge.driverId = drivers_day1.driverId
# MAGIC WHEN MATCHED THEN
# MAGIC   UPDATE SET drivers_merge.dob = drivers_day1.dob,
# MAGIC              drivers_merge.forename = drivers_day1.forename,
# MAGIC              drivers_merge.surname = drivers_day1.surname,
# MAGIC              drivers_merge.updatedDate = current_timestamp  
# MAGIC WHEN NOT MATCHED THEN
# MAGIC   INSERT (driverId, dob, forename, surname, createdDate) 
# MAGIC   VALUES (drivers_day1.driverId, drivers_day1.dob, drivers_day1.forename, 
# MAGIC           drivers_day1.surname, current_timestamp);

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT *
# MAGIC FROM f1_demo.drivers_merge;

# COMMAND ----------

# MAGIC %sql
# MAGIC MERGE INTO f1_demo.drivers_merge
# MAGIC USING drivers_day2
# MAGIC   ON drivers_merge.driverId = drivers_day2.driverId
# MAGIC WHEN MATCHED THEN
# MAGIC   UPDATE SET drivers_merge.dob = drivers_day2.dob,
# MAGIC              drivers_merge.forename = drivers_day2.forename,
# MAGIC              drivers_merge.surname = drivers_day2.surname,
# MAGIC              drivers_merge.updatedDate = current_timestamp  
# MAGIC WHEN NOT MATCHED THEN
# MAGIC   INSERT (driverId, dob, forename, surname, createdDate) 
# MAGIC   VALUES (drivers_day2.driverId, drivers_day2.dob, drivers_day2.forename, 
# MAGIC           drivers_day2.surname, current_timestamp);

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT *
# MAGIC FROM f1_demo.drivers_merge;

# COMMAND ----------

from pyspark.sql.functions import current_timestamp
from delta.tables import DeltaTable

deltaTable = DeltaTable.forPath(spark, "/mnt/formula1pw/demo/drivers_merge")

deltaTable.alias("target").merge(drivers_day3_df.alias("source"),
                                    "source.driverId = target.driverId") \
                          .whenMatchedUpdate( set = { "dob": "source.dob", "forename": "source.forename", 
                                                      "surname": "source.surname", "updatedDate": "current_timestamp()"
                                                    }) \
                          .whenNotMatchedInsert( values = { "driverId": "source.driverId", "dob": "source.dob",
                                                            "forename": "source.forename", "surname": "source.surname",
                                                            "createdDate": "current_timestamp()"
                                                            }) \
                          .execute()

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * 
# MAGIC FROM f1_demo.drivers_merge;

# COMMAND ----------

# MAGIC %md
# MAGIC #History and Versioning

# COMMAND ----------

# MAGIC %sql
# MAGIC DESC HISTORY f1_demo.drivers_merge;

# COMMAND ----------

# MAGIC %md
# MAGIC #Time travel
# MAGIC - using SQL
# MAGIC - using PySpark

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM f1_demo.drivers_merge VERSION AS OF 1;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * 
# MAGIC FROM f1_demo.drivers_merge 
# MAGIC TIMESTAMP AS OF '2024-09-09T19:05:20.000+00:00';

# COMMAND ----------

df = spark.read.format("delta") \
               .option("timestampAsOf", '2024-09-09T19:05:20.000+00:00') \
               .load("/mnt/formula1pw/demo/drivers_merge")

# COMMAND ----------

display(df)

# COMMAND ----------

# MAGIC %md
# MAGIC #Vaccum
# MAGIC - removes all history of the delta table (default retain data to 7 days)

# COMMAND ----------

# MAGIC %sql
# MAGIC VACUUM f1_demo.drivers_merge;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * 
# MAGIC FROM f1_demo.drivers_merge 
# MAGIC TIMESTAMP AS OF '2024-09-09T19:05:20.000+00:00';

# COMMAND ----------

# MAGIC %sql
# MAGIC SET spark.databricks.delta.retentionDurationCheck.enabled = false;
# MAGIC VACUUM f1_demo.drivers_merge RETAIN 0 HOURS;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * 
# MAGIC FROM f1_demo.drivers_merge 
# MAGIC TIMESTAMP AS OF '2024-09-09T19:05:20.000+00:00';

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * 
# MAGIC FROM f1_demo.drivers_merge 

# COMMAND ----------

# MAGIC %sql
# MAGIC DESC HISTORY f1_demo.drivers_merge;

# COMMAND ----------

# MAGIC %md
# MAGIC #Restore deleted data

# COMMAND ----------

# MAGIC %sql
# MAGIC DELETE
# MAGIC FROM f1_demo.drivers_merge
# MAGIC WHERE driverId = 1;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * 
# MAGIC FROM f1_demo.drivers_merge 
# MAGIC VERSION AS OF 3;

# COMMAND ----------

# MAGIC %sql
# MAGIC MERGE INTO f1_demo.drivers_merge as target
# MAGIC USING f1_demo.drivers_merge VERSION AS OF 3 as source
# MAGIC   ON target.driverId = source.driverId
# MAGIC WHEN NOT MATCHED THEN
# MAGIC   INSERT *;  

# COMMAND ----------

# MAGIC %sql
# MAGIC DESC HISTORY f1_demo.drivers_merge

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM f1_demo.drivers_merge;

# COMMAND ----------

# MAGIC %md
# MAGIC #Transaction Logs
# MAGIC - default they are kept for 30 days

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS f1_demo.drivers_txn (
# MAGIC   driver_id INT,
# MAGIC   dob DATE,
# MAGIC   forename STRING,
# MAGIC   surname STRING,
# MAGIC   createdDate DATE,
# MAGIC   updatedDate DATE
# MAGIC )
# MAGIC USING DELTA

# COMMAND ----------

# MAGIC %sql
# MAGIC DESC HISTORY f1_demo.drivers_txn

# COMMAND ----------

# MAGIC %sql
# MAGIC INSERT INTO f1_demo.drivers_txn
# MAGIC SELECT * FROM f1_demo.drivers_merge
# MAGIC WHERE driverId = 1;

# COMMAND ----------

# MAGIC %sql
# MAGIC DESC HISTORY f1_demo.drivers_txn

# COMMAND ----------

# MAGIC %sql
# MAGIC INSERT INTO f1_demo.drivers_txn
# MAGIC SELECT * FROM f1_demo.drivers_merge
# MAGIC WHERE driverId = 2;

# COMMAND ----------

# MAGIC %sql
# MAGIC DESC HISTORY f1_demo.drivers_txn

# COMMAND ----------

# MAGIC %sql
# MAGIC DELETE FROM f1_demo.drivers_txn
# MAGIC WHERE driver_id = 1;

# COMMAND ----------

for driver_id in range(3,20):
    spark.sql(f"""INSERT INTO f1_demo.drivers_txn
                  SELECT * FROM f1_demo.drivers_merge
                  WHERE driverId = {driver_id}""")

# COMMAND ----------

# MAGIC %md
# MAGIC #Convert Parquet to Delta

# COMMAND ----------

# MAGIC %md
# MAGIC Convert Parquet to Delta using Table

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS f1_demo.drivers_convert_to_delta(
# MAGIC   driverId INT,
# MAGIC   dob DATE,
# MAGIC   forename STRING,
# MAGIC   surname STRING,
# MAGIC   createdDate DATE,
# MAGIC   updatedDate DATE
# MAGIC )
# MAGIC USING PARQUET

# COMMAND ----------

# MAGIC %sql
# MAGIC INSERT INTO f1_demo.drivers_convert_to_delta
# MAGIC SELECT * FROM f1_demo.drivers_merge

# COMMAND ----------

# MAGIC %sql
# MAGIC CONVERT TO DELTA f1_demo.drivers_convert_to_delta

# COMMAND ----------

# MAGIC %md
# MAGIC Convert Parquet to Delta without using Table

# COMMAND ----------

df = spark.table("f1_demo.drivers_convert_to_delta")

# COMMAND ----------

df.write.format("parquet") \
        .save("/mnt/formula1pw/demo/drivers_convert_to_delta_new")

# COMMAND ----------

# MAGIC %sql
# MAGIC CONVERT TO DELTA parquet.`/mnt/formula1pw/demo/drivers_convert_to_delta_new`
