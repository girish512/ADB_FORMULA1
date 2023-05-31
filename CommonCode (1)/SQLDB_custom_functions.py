# Databricks notebook source
def fetch_sqltable_data(sql_query):
    table_data = spark.read.format("jdbc")\
                        .option("url","jdbc:sqlserver://serverdbformula1.database.windows.net:1433;database=DB_FORMULA1;user=adm@serverdbformula1;password=Girish512@")\
                        .option("query",sql_query)\
                        .load()
    return(table_data)

