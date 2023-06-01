# Databricks notebook source
dbutils.secrets.help()

# COMMAND ----------

dbutils.secrets.get(scope="projectFormula1-scope",key="dlconnectionkey")
dbutils.secrets.list(scope="projectFormula1-scope")

# COMMAND ----------


