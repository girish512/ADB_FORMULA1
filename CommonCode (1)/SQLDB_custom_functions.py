# Databricks notebook source
def fetch_sqltable_data(qry, partition_column, lower_bound, upper_bound, num_partitions):
    table_data = spark.read.format("jdbc")\
                        .option("url","jdbc:sqlserver://serverdbformula1.database.windows.net:1433;database=DB_FORMULA1;user=adm@serverdbformula1;password=Girish512@")\
                        .option("partitionColumn",partition_column)\
                        .option("lowerBound",lower_bound)\
                        .option("upperBound",upper_bound)\
                        .option("numPartitions",num_partitions)\
                        .option("dbtable",f"({qry}) as outtbl")\
                        .load()
    return(table_data)


# COMMAND ----------

def create_temp_table (df,tmp_table):
    table_load = df.write.format("jdbc").\
                            option("url","jdbc:sqlserver://serverdbformula1.database.windows.net:1433;database=DB_FORMULA1;user=adm@serverdbformula1;password=Girish512@").\
                            option("dbtable",tmp_table).\
                            option("user","adm").\
                            option("password","Girish512@").\
                            save()
    return(table_load)



# COMMAND ----------


                      #  .option("partitionColumn",partition_column)\
                      #  .option("lowerBound",lower_bound)\
                      #  .option("upperBound",upper_bound)\
                      #  .option("numPartitions",num_partitions)\
