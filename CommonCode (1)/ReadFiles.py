# Databricks notebook source
# MAGIC %md
# MAGIC # Read All Files in the Project

# COMMAND ----------

# Imports
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DecimalType, Row
from pyspark.sql.functions import col, current_timestamp

# COMMAND ----------

# FIle Schemas

InputSourcePath = dbutils.secrets.get(scope="projectFormula1-scope", key="InputFilePath")
CircuitsFileName = dbutils.secrets.get(scope="projectFormula1-scope", key="circuitsFileName")
RacesFileName = dbutils.secrets.get(scope="projectFormula1-scope", key="racesFileName")

circuitsFileSchema = StructType([StructField("circuitId", IntegerType(),True), \
                    StructField("circuitRef", StringType(),True), \
                    StructField("name", StringType(),True), \
                    StructField("location", StringType(),True), \
                    StructField("country", StringType(),True), \
                    StructField("lat", DecimalType(),True), \
                    StructField("lng", DecimalType(),True), \
                    StructField("alt", IntegerType(),True), \
                    StructField("url", StringType(),True),\
                    StructField("corrupt_record", StringType(),True)]);

racesFileSchema = StructType([StructField("raceId", IntegerType(), True),\
                  StructField("year", IntegerType(), True),\
                  StructField("round", IntegerType(), True),\
                  StructField("circuitId", IntegerType(), True),\
                  StructField("name", StringType(), True),\
                  StructField("date", StringType(), True),\
                  StructField("time", StringType(), True),\
                  StructField("url", StringType(), True)]);

resultsFileSchema = StructType([StructField("resultId", IntegerType(), False),\
                                StructField("raceId", IntegerType(), False),\
                                StructField("driverId", IntegerType(), False),\
                                StructField("constructorId", IntegerType(), False),\
                                StructField("number", IntegerType(), False),\
                                StructField("grid", IntegerType(), False),\
                                StructField("position", IntegerType(), False),\
                                StructField("positionText", StringType(), False),\
                                StructField("positionOrder", StringType(), False),\
                                StructField("points", IntegerType(), False),\
                                StructField("laps", IntegerType(), False),\
                                StructField("time", StringType(), False),\
                                StructField("milliseconds", IntegerType(), False),\
                                StructField("fastestLap", IntegerType(), False),\
                                StructField("fastestLapTime", StringType(), False),\
                                StructField("fastestLapSpeed", DecimalType(), False),\
                                StructField("statusId", IntegerType(), False)])
                  

# COMMAND ----------

# Circuit FIle DataFrames
#------------------------------------------------

df_circuitfile=spark.read \
.option("header", True) \
.option("inferSchema", True) \
.option("mode","PERMISSIVE")\
.option("columnNameOfCorruptRecord","corrupt_record")\
.schema(circuitsFileSchema) \
.csv(f"{InputSourcePath}/f1db_csv/{CircuitsFileName}")


# Race File DataFrames
#------------------------------------------------\
df_racefile = spark.read\
.option("header", True)\
.option("inferSchema", True)\
.schema(racesFileSchema)\
.csv(f"{InputSourcePath}/f1db_csv/{RacesFileName}")

#Results File DataFrames
def fn_df_resultfile(ingestionDate):
    df_resultfile_out = spark.read\
    .option("header", True)\
    .option("inferSchema", True)\
    .schema(resultsFileSchema)\
    .json(f"{InputSourcePath}/f1db_csv/{ingestionDate}/results.json")
    return df_resultfile_out




# COMMAND ----------

dbutils.notebook.exit("Success")
