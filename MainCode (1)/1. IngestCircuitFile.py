# Databricks notebook source
# MAGIC %run "../CommonCode/ReadFiles"

# COMMAND ----------

# Imports
from pyspark.sql.functions import current_timestamp
from pyspark.sql.dataframe import DataFrameWriter

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
#dfFinal_circuitFile.show()


# COMMAND ----------

# Load Circuit File to ADLS Parquet Format
dfFinal_circuitFile.write\
.mode("overwrite")\
.parquet("/mnt/dlgirishproject/data/Files/Output/circuit")

#dbutils.fs.mounts()



# COMMAND ----------

dfFinal_circuitFile.write\
.mode("overwrite")\
.format("delta")\
.saveAsTable("f1_project_db.circuit")

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC SELECT count(*) FROM f1_project_db.results_mng
