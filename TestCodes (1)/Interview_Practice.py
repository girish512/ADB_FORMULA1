# Databricks notebook source
# MAGIC %run "../CommonCode (1)/ReadFiles"

# COMMAND ----------

from pyspark.sql.functions import col,current_timestamp,filter
from pyspark.sql.types import StructType,StructField,IntegerType,StringType,DecimalType,Row

# COMMAND ----------

Results_Schema = StructType([StructField("resultId", IntegerType(), False),\
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
                                StructField("statusId", IntegerType(), False),\
                                StructField("corrupt_record", StringType(), False)])
                  

# COMMAND ----------


InputSourcePath = dbutils.secrets.get(scope="projectFormula1-scope", key="InputFilePath")

df_results = spark.read\
        .option("header", True)\
        .option("mode","PERMISSIVE")\
        .option("columnNameOfCorruptRecord","corrupt_record")\
        .schema(Results_Schema)\
        .json(f"{InputSourcePath}/f1db_csv/2021-03-21/results.json");
df_results_renamed = df_results.withColumnRenamed("raceId","rs_race_id")
df_racefile_renamed = df_racefile.withColumnRenamed("raceId","rc_race_id")

df_result_circuit = df_results_renamed\
                    .join(df_racefile_renamed,df_results_renamed.rs_race_id==df_racefile_renamed.rc_race_id,'Inner')\
                    .select(df_results_renamed.driverId,df_racefile_renamed.name,df_results_renamed.points)

df_rs_c_grp = df_result_circuit.groupBy("driverId","name")\
                .sum("points")\
                .withColumnRenamed("sum(points)","total_points")\
                .filter("total_points>=100")
                   
df_totalPoints = df_result_circuit.agg(sum(df_result_circuit.points))
df_result_circuit.show(10)
display(df_rs_c_grp)
