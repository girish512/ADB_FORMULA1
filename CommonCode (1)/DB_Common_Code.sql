-- Databricks notebook source
-- MAGIC %sql
-- MAGIC CREATE DATABASE  IF NOT EXISTS F1_PROJECT_DB
-- MAGIC LOCATION  "/mnt/dlgirishproject/data/Database/"

-- COMMAND ----------

-- MAGIC %sql
-- MAGIC SHOW DATABASES

-- COMMAND ----------

-- MAGIC %sql
-- MAGIC SELECT current_database()

-- COMMAND ----------

-- MAGIC %sql
-- MAGIC DESC DATABASE F1_PROJECT_DB
-- MAGIC

-- COMMAND ----------

USE DATABASE F1_PROJECT_DB

-- COMMAND ----------

SELECT current_database()


-- COMMAND ----------

CREATE TABLE IF NOT EXISTS f1_project_db.RESULTS
(resultId INT,
 raceId INT,
  driverId INT,
  constructorId INT ,
  position INT,
  points INT,
  created_date TIMESTAMP,
  updated_date TIMESTAMP,
  file_date STRING)
USING DELTA
LOCATION '/mnt/dlgirishproject/data/Database/results'
--AS select * from tv_results where constructorId =1


-- COMMAND ----------

-- MAGIC %python
-- MAGIC if(spark.sql("DROP TABLE IF EXISTS f1_project_db.RESULTS")):\
-- MAGIC dbutils.fs.rm('/mnt/dlgirishproject/data/Database/results',recurse=True)
-- MAGIC # DROP TABLE f1_project_db.RESULTS --> IT WILL DROP THE TABLE. BUT NOT THE STORAGE FILES. sO WE HAVE TO USE ABOVE COMMAND.
