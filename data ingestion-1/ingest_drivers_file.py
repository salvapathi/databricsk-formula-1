# Databricks notebook source
# MAGIC %md
# MAGIC ### reading nested json file to spark dataframe which is the drivers data 

# COMMAND ----------

#defining the schema 
#renaming the columns 
#adding the ingested date
#dropping the unwanted colums
from pyspark.sql.types import *
from pyspark.sql.functions import *
from pyspark.sql.functions import col, concat, lit, to_timestamp

# COMMAND ----------

name_schema=StructType(fields=[StructField("forename",StringType(),False),
                                StructField("surname",StringType(),False)
                                ])
driver_schema = StructType(fields=[
    StructField("driverId", IntegerType(), False),
    StructField("driverRef", StringType(), True),
    StructField("number", IntegerType(), True),
    StructField("code", StringType(), True),
    StructField("name", name_schema, True),
    StructField("dob", DateType(), True),  
    StructField("nationality", StringType(), True),
    StructField("url", StringType(), True)  # Removed the extra comma here
])

driver_df=spark.read.schema(driver_schema)\
    .json("/mnt/rawdata/drivers.json")

# COMMAND ----------


driver_df = driver_df.select(
    col("driverId").alias("driver_id"),
    col("driverRef").alias("driver_ref"),
    col("number"),
    col("code"),
    concat(col("name.forename"), lit(" "), col("name.surname")).alias("name"),
    col("dob"),
    col("nationality")
)

# COMMAND ----------

driver_df.write.mode("overwrite").parquet("mnt/silverdata/driverfile")

# COMMAND ----------

display(driver_df)

# COMMAND ----------


