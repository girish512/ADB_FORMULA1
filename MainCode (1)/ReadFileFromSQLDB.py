# Databricks notebook source
# MAGIC %run "../CommonCode/ReadFiles"

# COMMAND ----------

# MAGIC %run "../CommonCode/SQLDB_custom_functions"

# COMMAND ----------

# Imports
from pyspark.sql.functions import current_timestamp,col
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
df_val_circuitID = select_circuitFile_DF.select("circuit_id").collect()

tb_circuit_df=fetch_sqltable_data("select * from circuit")

if (tb_circuit_df.count()>0):
            operation = "update"
else:
            operation = "insert"
            

for circuit in df_val_circuitID:\
    sql_query = (f"SELECT count(*)as abc FROM DB_FORMULA1.DBO.CIRCUIT WHERE CIRCUIT_ID = '{circuit.circuit_id}'");\
    tb_circuit_df=fetch_sqltable_data(sql_query);\
    if (tb_circuit_df.count()>0):
                operation = "update"
                print(operation)
    else:
                operation = "insert"
                print(operation)
    




# COMMAND ----------

tb_circuit_file = spark.read.format("jdbc")\
    .option("url","jdbc:sqlserver://serverdbformula1.database.windows.net:1433;database=DB_FORMULA1;user=adm@serverdbformula1;password=Girish512@")\
    .option("query",f"SELECT * FROM DB_FORMULA1.DBO.CIRCUIT WHERE CIRCUIT_ID in {df_val_circuitID}")\
    .load()

# COMMAND ----------


