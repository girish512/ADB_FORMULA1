# Databricks notebook source
# MAGIC %run "../CommonCode (1)/ReadFiles"

# COMMAND ----------

# MAGIC %run "../CommonCode (1)/SQLDB_custom_functions"

# COMMAND ----------

# Imports
from pyspark.sql.functions import current_timestamp,col,lit,when
from pyspark.sql.dataframe import DataFrameWriter


# COMMAND ----------

#Read circuitfile table from sql db
tb_circuit_df=fetch_sqltable_data("select * from circuit","circuit_id","0","10","1")\
.withColumnRenamed("circuit_id","tb_circuit_id")\
.select("tb_circuit_id")

# COMMAND ----------

# Read Circuit File csv and adding ingestion date
select_circuitFile_DF = df_circuitfile.select(col("circuitId").alias("circuit_id"),\
col("circuitRef").alias("circuit_ref"),\
col("name").alias("circuit_name"),\
col("location"),\
col("country"),\
col("lat").alias("latitude"),\
col("lng").alias("longitude"),\
col("alt").alias("altitude"))

dfFinal_circuitFile = select_circuitFile_DF.withColumn("ingested_date", current_timestamp())
display(dfFinal_circuitFile)

#adding additional field to know whether the record is of update or insert

tb_df_df = dfFinal_circuitFile.join(tb_circuit_df,\
                             dfFinal_circuitFile.circuit_id==tb_circuit_df.tb_circuit_id,\
                                 'left_outer').\
                        withColumn("operation", when(dfFinal_circuitFile.circuit_id==tb_circuit_df.tb_circuit_id,"update").otherwise("insert"))

# selecting required columns to create temp table in the db
circuit_final_df = tb_df_df.select(tb_df_df.circuit_id,\
                        tb_df_df.circuit_name,
                        tb_df_df.location,\
                        tb_df_df.country,\
                        tb_df_df.latitude,\
                        tb_df_df.longitude,\
                        tb_df_df.altitude,\
                        tb_df_df.ingested_date,\
                        tb_df_df.operation)

display(tb_df_df)                        


create_temp_table(circuit_final_df,"circuit_tmp")



            






# COMMAND ----------

# MAGIC %sql  
# MAGIC select * from vw_circuit
