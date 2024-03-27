# Databricks notebook source
# MAGIC %md
# MAGIC ## Reading the json formate file from constructore table 
# MAGIC ## data cleanin the schema 
# MAGIC ##### reading json formate  with the help of spark 

# COMMAND ----------

#ingesting the data
from pyspark.sql.types import *
from pyspark.sql.functions import *
from pyspark.sql.functions import col, concat, lit, to_timestamp
from pyspark.sql.functions import current_timestamp, from_utc_timestamp
display(dbutils.fs.ls("/mnt/rawdata"))

# COMMAND ----------

constructor_schema="constructorId INT,constructorRef STRING,name STRING,nationality STRING,url STRING"
constructor_df=spark.read\
    .schema(constructor_schema)\
        .json("/mnt/rawdata/constructors.json")
display(constructor_df)

# COMMAND ----------

#dropping the unwanted column
#constructor_df.drop(constructor_df["url"])
constructor_df=constructor_df.select(col("constructorId").alias("constructor_id"),col("constructorRef").alias("constructor_ref"),\
    col("name"),col("nationality")).withColumn("ingested_date", from_utc_timestamp(current_timestamp(), "IST"))
display(constructor_df)

# COMMAND ----------

constructor_df.write.mode("overwrite").parquet("/mnt/silverdata/constructors")
