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

# MAGIC %md
# MAGIC ## To  specify fields and data types Schema

# COMMAND ----------

races_schema=StructType(fields=[StructField("raceId",IntegerType(),False),
                                   StructField("year",StringType(),True),
                                   StructField("round",IntegerType(),True),
                                   StructField("circuitId",StringType(),False),
                                   StructField("name",StringType(),True),
                                   StructField("date",DateType(),True),
                                   StructField("time",StringType(),True),
                                   StructField("url",StringType(),True)
                                   ])
races_df=spark.read.csv("/mnt/rawdata/races.csv",schema=races_schema,header=True)
races_df.printSchema()
display(races_df)

# COMMAND ----------

#changing the column names and dropping the unwanted columns with the help of col function and alias function
from pyspark.sql.functions import col
races_selected_df=races_df.select(col("raceId").alias("race_id"),col("year").alias("race_year"),
                                       col("round"),col("circuitId").alias("circuit_id"),col("name"),col("date").alias("race_date"),col("time").alias("race_time"))
display(races_selected_df)
                          

# COMMAND ----------

# MAGIC %md 
# MAGIC ##### for every ingestion there should be data that when ingested 

# COMMAND ----------

#timestamp ingestion 
display(races_selected_df.withColumn("race_timestamp", to_timestamp(concat(col("race_date"), lit(" "), col("race_time")), "yyyy-MM-dd HH:mm:ss")))


# COMMAND ----------

#write data to parquet
#races_selected_df.write.mode("overwrite").parquet("/mnt/silverdata/races")

# COMMAND ----------

#storing the data in adls gen 2 with the help of partition
races_selected_df.write.mode("overwrite").partitionBy("race_year").parquet("/mnt/silverdata/races")
