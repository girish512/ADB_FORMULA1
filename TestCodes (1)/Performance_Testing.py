# Databricks notebook source
# MAGIC %run "../CommonCode (1)/ReadFiles"

# COMMAND ----------

from pyspark.sql.functions import count

# COMMAND ----------

df_results = fn_df_resultfile('2021-03-21')
df_racefile.createOrReplaceTempView("tmp_vw_race")


df_results.createOrReplaceTempView("tmp_vw_results")

res_Race_join = spark.sql("select tres.raceid, trc.name as racename, count(tres.driverId) drivers, sum(tres.points) points from tmp_vw_results tres,tmp_vw_race trc where trc.raceId=tres.raceId group by tres.raceid, trc.name")
display(res_Race_join)

results_agg = df_results.select("resultId","raceId").\
    groupBy("resultId").\
    agg(count("raceId"),count("resultId"))
results_agg.count()
display(results_agg)

#df_results.unpersist()

# COMMAND ----------

df_results_p = fn_df_resultfile('2021-03-21').cache()



results_agg_p = df_results_p.select("resultId","raceId").\
    groupBy("resultId").\
    agg(count("raceId"),count("resultId"))
results_agg_p.count()
df_results_p.unpersist()

display(results_agg_p)
