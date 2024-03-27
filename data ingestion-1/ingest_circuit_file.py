# Databricks notebook source
# MAGIC %md
# MAGIC #Data Ingestion
# MAGIC ### Read the csv file fmore datalake gen2 with the help of mount 
# MAGIC ### process : 
# MAGIC               1.load 
# MAGIC               2.Transform
# MAGIC               3.ingest -in parque

# COMMAND ----------

from pyspark.sql.types import *
from pyspark.sql.functions import *
from pyspark.sql.functions import col, concat, lit, to_timestamp


# COMMAND ----------

#display the mounts and source from the data
display(dbutils.fs.mounts())

# COMMAND ----------

display(dbutils.fs.ls("/mnt/rawdata"))

# COMMAND ----------

#step-1 reading the file in Spark
circuits_df=spark.read.csv("dbfs:/mnt/rawdata/circuits.csv",inferSchema=True,header=True)
circuits_df.printSchema()
display(circuits_df)

# COMMAND ----------

display(circuits_df.describe())

# COMMAND ----------

# MAGIC %md
# MAGIC ## To  specify fields and data types Schema

# COMMAND ----------

circuits_schema=StructType(fields=[StructField("circuitId",IntegerType(),True),
                                   StructField("circuitRef",StringType(),False),
                                   StructField("name",StringType(),False),
                                   StructField("location",StringType(),False),
                                   StructField("country",StringType(),False),
                                   StructField("lat",DoubleType(),False),
                                   StructField("lng",DoubleType(),False),
                                   StructField("alt",DoubleType(),False),
                                   StructField("url",StringType(),False)
                                   ])
circuits_df=spark.read.csv("/mnt/rawdata/circuits.csv",schema=circuits_schema,header=True)
circuits_df.printSchema()
display(circuits_df)

# COMMAND ----------

#changing the column names and dropping the unwanted columns with the help of col function and alias function

circuits_selecteddf=circuits_df.select(col("circuitId").alias("circuit_id"),col("circuitRef").alias("circuit_ref"),
                                       col("name"),col("location"),col("country"),col("lat").alias("latitude"),col("lng").alias("longitude"),col("alt").alias("altitude"))
display(circuits_selecteddf)
                          

# COMMAND ----------

# MAGIC %md 
# MAGIC ##### for every ingestion there should be data that when ingested 

# COMMAND ----------

#timestamp ingestion 
from pyspark.sql.functions import current_timestamp, from_utc_timestamp

# Assuming you're using PySpark
circuits_selected_df=circuits_selecteddf.withColumn("ingested_date", 
                                                     from_utc_timestamp(current_timestamp(), "IST"))
circuits_selected_df.show()                                               

# COMMAND ----------

#write data to parquet
circuits_selected_df.write.mode("overwrite").parquet("mnt/silverdata/circuits_test")

# COMMAND ----------

df=spark.read.parquet("/mnt/silverdata/circuits")
df.show()

# COMMAND ----------



# COMMAND ----------


