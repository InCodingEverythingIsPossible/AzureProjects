# Databricks notebook source
dbutils.fs.ls('/mnt/bronze/SalesLT/')

# COMMAND ----------

dbutils.fs.ls('mnt/silver/')

# COMMAND ----------

input_path = '/mnt/bronze/SalesLT/Address/Address.parquet'

# COMMAND ----------

df = spark.read.format('parquet').load(input_path)

# COMMAND ----------

display(df)

# COMMAND ----------

from pyspark.sql.functions import from_utc_timestamp, date_format, col
from pyspark.sql.types import TimestampType

spark.conf.set("spark.sql.legacy.timeParserPolicy", "LEGACY")

df = df.withColumn("ModifiedDate", date_format(col("ModifiedDate"), "yyyy-MM-dd"))

# COMMAND ----------

display(df)

# COMMAND ----------

# MAGIC %md
# MAGIC #**Transformation for all tables**

# COMMAND ----------

table_name = []
for i in dbutils.fs.ls('mnt/bronze/SalesLT/'):
    table_name.append(i.name.split("/")[0])

# COMMAND ----------

print(table_name)

# COMMAND ----------

from pyspark.sql.functions import from_utc_timestamp, date_format, col
from pyspark.sql.types import TimestampType

for i in table_name:
    path = '/mnt/bronze/SalesLT/'+i+'/'+i+'.parquet'
    df = spark.read.format('parquet').load(path)
    columns = df.columns

    for column in columns:
        if "Date" in column or "date" in column:
            df = df.withColumn(column, date_format(col(column), "yyyy-MM-dd"))
    
    output_path = '/mnt/silver/SalesLT/'+i+'/'
    df.write.format('delta').mode("overwrite").save(output_path)

# COMMAND ----------

display(df)

# COMMAND ----------

dbutils.fs.ls('mnt/silver/SalesLT')
