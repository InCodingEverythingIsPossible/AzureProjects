# Databricks notebook source
# MAGIC %md
# MAGIC # Calculated race results

# COMMAND ----------

# MAGIC %md
# MAGIC # Passing parameter to the notebook

# COMMAND ----------

dbutils.widgets.text("file_date", "2021-03-21")
file_date = dbutils.widgets.get("file_date")

# COMMAND ----------

# MAGIC %md
# MAGIC #Calculate race results on Parquet
# MAGIC - without incremental load

# COMMAND ----------

# %sql
# CREATE TABLE f1_presentation.calculated_race_results
# USING PARQUET
# AS
# SELECT races.race_year,
#        constructors.name AS team_name,
#        drivers.name AS driver_name,
#        results.position,
#        results.points,
#        11 - results.position AS calculated_points
# FROM f1_processed.results
# JOIN f1_processed.drivers
#       ON results.driver_id = drivers.driver_id
# JOIN f1_processed.constructors
#       ON results.constructor_id = constructors.constructor_id
# JOIN f1_processed.races
#       ON results.race_id = races.race_id
# WHERE results.position <= 10

# COMMAND ----------

# MAGIC %md
# MAGIC #Calculate race results on Delta
# MAGIC - with incremental load

# COMMAND ----------

spark.sql(f"""
            CREATE TABLE IF NOT EXISTS f1_presentation.calculated_race_results(
                race_year INT,
                team_name STRING,
                driver_id INT,
                driver_name STRING,
                race_id INT,
                position INT,
                points INT,
                calculated_points INT,
                created_date TIMESTAMP,
                updated_date TIMESTAMP
            )
            USING DELTA
        """)

# COMMAND ----------

spark.sql(f"""
            CREATE OR REPLACE TEMPORARY VIEW race_results_updated
            AS
            SELECT races.race_year,
                constructors.name AS team_name,
                drivers.driver_id,
                drivers.name AS driver_name,
                races.race_id,
                results.position,
                results.points,
                11 - results.position AS calculated_points
            FROM f1_processed.results
            JOIN f1_processed.drivers
                ON results.driver_id = drivers.driver_id
            JOIN f1_processed.constructors
                ON results.constructor_id = constructors.constructor_id
            JOIN f1_processed.races
                ON results.race_id = races.race_id
            WHERE results.position <= 10
                AND results.file_date = '{file_date}'
        """)

# COMMAND ----------

spark.sql(f"""
            MERGE INTO f1_presentation.calculated_race_results AS target
            USING race_results_updated AS source
            ON (target.race_id = source.race_id AND
                target.driver_id = source.driver_id)
            WHEN MATCHED THEN
            UPDATE SET target.position = source.position,
                        target.points = source.points,
                        target.calculated_points = source.calculated_points,
                        target.updated_date = current_timestamp
            WHEN NOT MATCHED THEN
            INSERT (race_year, team_name, driver_id, driver_name, race_id, position, points, calculated_points, created_date)
            VALUES (source.race_year, source.team_name, source.driver_id, source.driver_name, source.race_id, source.position, 
                    source.points, source.calculated_points, current_timestamp)
          """)

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT *
# MAGIC FROM f1_presentation.calculated_race_results;
