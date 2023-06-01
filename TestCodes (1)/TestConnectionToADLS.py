# Databricks notebook source
#Required Imports
from pyspark.sql.types import StructType, StructField, StringType
from pyspark.sql.functions import col, current_timestamp


# COMMAND ----------

# Building Schema for File


circuitsFileSchema = StructType([StructField("circuitId", StringType(),True),
StructField("circuitRef", StringType(),True),
StructField("name", StringType(),True),
StructField("location", StringType(),True),
StructField("country", StringType(),True)]);

# COMMAND ----------

# Adding Secrets to the variables

applicationID = dbutils.secrets.get(scope="projectFormula1-scope", key="SPADF-AppID-ClientID"); #ClientID
directoryID = dbutils.secrets.get(scope="projectFormula1-scope", key="SPADF-DirID-TenantID");   #TenantID
clientSecret = dbutils.secrets.get(scope="projectFormula1-scope", key="SPADFValue");  #ServicePrinciple Secret Value
storageAccount = dbutils.secrets.get(scope="projectFormula1-scope", key="DL-storageAccount");

# COMMAND ----------

# Spark Configuration Setup

spark.conf.set("fs.azure.account.auth.type.dlgirishproject.dfs.core.windows.net", "OAuth")
spark.conf.set("fs.azure.account.oauth.provider.type.dlgirishproject.dfs.core.windows.net", "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider")
spark.conf.set("fs.azure.account.oauth2.client.id.dlgirishproject.dfs.core.windows.net", applicationID)
spark.conf.set("fs.azure.account.oauth2.client.secret.dlgirishproject.dfs.core.windows.net", clientSecret)
spark.conf.set("fs.azure.account.oauth2.client.endpoint.dlgirishproject.dfs.core.windows.net", f"https://login.microsoftonline.com/{directoryID}/oauth2/token")

# COMMAND ----------

# Reading File from adls

display(dbutils.fs.ls("abfss://data@dlgirishproject.dfs.core.windows.net/Files/Input/f1db_csv/circuits.csv"))

display(dbutils.fs.ls("/"))
dbutils.fs.help()


# COMMAND ----------

# Read Data from Circuits File

DF_CircuitsFile = spark.read \
                    .option("header", True) \
                    .csv("abfss://data@dlgirishproject.dfs.core.windows.net/Files/Input/f1db_csv/circuits.csv")

SelectColumns = DF_CircuitsFile.select(col("circuitId").alias("circuit_Id"),\
                                        col("circuitRef").alias("circuit_Ref"),\
                                        col("name"),\
                                        col("location"),\
                                        col("country"),\
                                        col("lat").alias("latitude"),\
                                        col("lng").alias("longitude"),\
                                        col("alt").alias("altitude"))

SelectColumns = SelectColumns.withColumn("ingestion_time", current_timestamp())

display(SelectColumns)
