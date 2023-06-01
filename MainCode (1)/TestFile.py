# Databricks notebook source
#Imports
from pyspark.sql.functions import when, col, sum, asc, desc
from pyspark.sql.types import DataType, StringType, List
from pyspark.sql import window
from pyspark.sql import DataFrame

# COMMAND ----------

# Read Parquet File
circuitsDF_Parquet = spark.read\
    .option("header", True)\
    .parquet("/mnt/dlgirishproject/data/Files/Output/circuit")

racesDF_Parquet = spark.read\
    .option("header", True)\
    .parquet("/mnt/dlgirishproject/data/Files/Output/races")

results_csv = spark.read\
    .option("header", True)\
    .csv("/mnt/dlgirishproject/data/Files/Input/f1db_csv/results.csv")

display(circuitsDF_Parquet.count())
display(racesDF_Parquet.count())
display(racesDF_Parquet)
display(results_csv)

# COMMAND ----------

#Joining DataFrames
races_Details = racesDF_Parquet.join(circuitsDF_Parquet,racesDF_Parquet.circuit_id==circuitsDF_Parquet.circuit_id, 'inner')\
    .join(results_csv,results_csv.raceId==racesDF_Parquet.race_id, 'inner')\
    .select (racesDF_Parquet.race_id,\
        racesDF_Parquet.name.alias("race_name"),\
        circuitsDF_Parquet.name.alias("circuit_name"),\
        racesDF_Parquet.race_year,\
        results_csv.points,\
        results_csv.position,\
        results_csv.driverId)

display(races_Details.filter("points !=0"))
races_Details.count()

# COMMAND ----------

# Case Statement
race_addcolumn = races_Details\
    .select("race_id",\
         "race_name", \
            "circuit_name", \
                when (races_Details.circuit_name == "Bahrain International Circuit",1).otherwise(0)\
                    .alias("T"))
display(race_addcolumn)

# COMMAND ----------

# Aggregation
display(races_Details)
races_Details.show()

driver_points = races_Details.select(col("race_year"),col("driverId").alias("driver_id"),col("race_name"),col("circuit_name"),col("points").cast('Integer'))\
    .filter("race_year == 2020")

driver_points.groupBy("race_year","driver_id","race_name","circuit_name")\
    .agg(sum("points")).show()

driver_points_results = driver_points.groupBy("race_year","driver_id","race_name","circuit_name")\
.sum("points")\
.withColumnRenamed("sum(points)","total_points")\
.sort(asc("driver_id"))

display(driver_points_results)
    


# COMMAND ----------

# Read List of Files

filelist_df=spark.createDataFrame(dbutils.fs.ls("/mnt/dlgirishproject/data/Files/Input/f1db_csv/filelist")) #converts list to df

#selects name from df and converts to list and iterate
for file_lst in filelist_df.select(col("name")).collect():\
    display(spark.read.option("header", True).csv(f"/mnt/dlgirishproject/data/Files/Input/f1db_csv/filelist/{file_lst[0]}"))
#same as above selects name from df and converts to list and iterate
for file_lst in filelist_df.select(col("name")).collect():\
    display(spark.read.option("header", True).csv(f"/mnt/dlgirishproject/data/Files/Input/f1db_csv/filelist/{file_lst.name}"))



# COMMAND ----------

for file_lst in filelist_df.select(col("name")).collect():\
    display(spark.read.option("header", True).csv(f"/mnt/dlgirishproject/data/Files/Input/f1db_csv/filelist/{file_lst.name}"))

# COMMAND ----------

#replace parquet file partition race_year date with current date

races_pq_df =  spark.read.parquet("/mnt/dlgirishproject/data/Files/Output/races").filter("race_year=1953")
replace_race_ingestiondate = races_pq_df.select("race_id",\
    "race_year",\
    "round",\
    "circuit_id",\
    "name",\
    "race_timestamp",\
    current_timestamp().alias("ingestion_date"))

replace_race_ingestiondate.write.\
   mode("overwrite")\
   .partitionBy("race_year")\
   .parquet("/mnt/dlgirishproject/data/Files/Output/races")

   display(races_pq_df)
dbutils.fs.rm("/mnt/dlgirishproject/data/Files/Output/races/race_year=1953",recurse=True)

# COMMAND ----------

#select distinct race_year from parquet file

races_pq_df =  spark.read.parquet("/mnt/dlgirishproject/data/Files/Output/races")
distinctraces = races_pq_df.select(col("race_year")).distinct()

display(distinctraces)
