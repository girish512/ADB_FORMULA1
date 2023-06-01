# Databricks notebook source
# MAGIC %run "../CommonCode/ReadFiles"

# COMMAND ----------


# Imports
from pyspark.sql.functions import current_timestamp
from pyspark.sql.dataframe import DataFrameWriter

# COMMAND ----------

InputSourcePath = dbutils.secrets.get(scope="projectFormula1-scope", key="InputFilePath")
CircuitsFileName = dbutils.secrets.get(scope="projectFormula1-scope", key="circuitsFileName")
RacesFileName = dbutils.secrets.get(scope="projectFormula1-scope", key="racesFileName")

# COMMAND ----------

df_circuitfile=spark.read \
.option("header", True) \
.option("inferSchema", True) \
.schema(circuitsFileSchema) \
.csv(f"{InputSourcePath}/f1db_csv/{CircuitsFileName}")

# COMMAND ----------


# Read Circuit File csv
select_circuitFile_DF = df_circuitfile.select(col("circuitId").alias("circuit_id"),\
col("circuitRef").alias("circuit_ref"),\
col("name"),\
col("location"),\
col("country"),\
col("lat").alias("latitude"),\
col("lng").alias("longitude"),\
col("alt").alias("altitude"))

dfFinal_circuitFile = select_circuitFile_DF.withColumn("ingested_date", current_timestamp())
display(dfFinal_circuitFile)


# COMMAND ----------

#write data to SQl Db
dfFinal_circuitFile.write.mode("append")\
    .jdbc("jdbc:sqlserver://serverdbformula1.database.windows.net:1433;database=DB_FORMULA1","DBO.CIRCUIT", properties={"user":"adm", "password":"Girish512@"})

# COMMAND ----------


