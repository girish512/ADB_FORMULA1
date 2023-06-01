# Databricks notebook source
#%run "../CommonCode/ReadFiles"
dbutils.notebook.run("/ProjectFormula1/CommonCode/ReadFiles",0)

# COMMAND ----------

#imports
from delta.tables import DeltaTable
from pyspark.sql.functions import current_timestamp, to_date, lit

# COMMAND ----------

#parameters
dbutils.widgets.text("fileDate","");
current_file = dbutils.widgets.get("fileDate")
print(lit(current_file))

# COMMAND ----------

#Update or insert to delta table results
resultfile_df = fn_df_resultfile(current_file).withColumn("file_date",lit(current_file))
resultfile_tb = DeltaTable.forPath(spark,'/mnt/dlgirishproject/data/Database/results')

resultfile_df.createOrReplaceTempView("vw_resultfile")

# DROP TABLE "temp_view_name"

resultfile_tb.alias('resultfile_tb')\
    .merge(resultfile_df.alias("src_results"),\
        "resultfile_tb.resultId =src_results.resultId")\
    .whenMatchedUpdate(set = {
        "resultId" : "src_results.resultId",
        "raceId" : "src_results.raceId",
        "driverId" : "src_results.driverId",
        "constructorId" : "src_results.constructorId",
        "position" : "src_results.position",
        "points" : "src_results.points",
        "updated_Date" : current_timestamp(),
        "file_date" : "src_results.file_date"})\
    .whenNotMatchedInsert(values = {"resultId" : "src_results.resultId",
        "raceId" : "src_results.raceId",
        "driverId" : "src_results.driverId",
        "constructorId" : "src_results.constructorId",
        "position" : "src_results.position",
        "points" : "src_results.points",
        "created_date" : current_timestamp(),
        "file_date" : "src_results.file_date"})\
    .execute()




# COMMAND ----------



# COMMAND ----------

# MAGIC %sql
# MAGIC --DELETE FROM f1_project_db.results
# MAGIC select * from f1_project_db.results
# MAGIC --where driverId is null
# MAGIC --group by file_date
# MAGIC --having count(file_date)>1
# MAGIC where file_date='2021-03-28'
# MAGIC --where updated_date IS not NULL
# MAGIC and raceId = 1052
# MAGIC
# MAGIC
# MAGIC
# MAGIC
# MAGIC
