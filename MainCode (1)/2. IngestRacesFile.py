# Databricks notebook source
# MAGIC %run "../CommonCode/ReadFiles"

# COMMAND ----------

#Imports
from pyspark.sql.functions import col, current_timestamp, concat, lit, to_timestamp, col
from pyspark.sql.dataframe import DataFrameWriter

# COMMAND ----------

# Read Races File CSV

select_racesfile_df = df_racefile.select(col("raceId").alias("race_id"),\
col("year").alias("race_year"),\
col("round"),\
col("circuitId").alias("circuit_id"),\
col("name"),\
col("date"),\
col("time"))

newcolunmsToDF = select_racesfile_df.withColumn("ingestion_date",current_timestamp())\
.withColumn("race_timestamp",concat(select_racesfile_df.date, lit(' '),select_racesfile_df.time))

raceFIleFinalDF = newcolunmsToDF.select("race_id",\
    "race_year",\
    "round",\
    "circuit_id",\
    "name",\
    "race_timestamp",\
    "ingestion_date")

#raceFIleFinalDF.show()



# COMMAND ----------

# Write Races File to ADLS in Parquet Format
raceFIleFinalDF.write\
    .mode("overwrite")\
    .partitionBy("race_year")\
    .parquet("/mnt/dlgirishproject/data/Files/Output/races")

#racefile_df = spark.read.parquet("/mnt/dlgirishproject/data/Files/Output/races)

