# Databricks notebook source
# MAGIC %md
# MAGIC # Mounting ADLS To DBFS
# MAGIC 1. Create Configs
# MAGIC 2. Mounting ADLS to DBFS

# COMMAND ----------

# Create Configs

configs = {"fs.azure.account.auth.type": "OAuth",
          "fs.azure.account.oauth.provider.type": "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider",
          "fs.azure.account.oauth2.client.id": dbutils.secrets.get(scope="projectFormula1-scope", key="SPADF-AppID-ClientID"),
          "fs.azure.account.oauth2.client.secret": dbutils.secrets.get(scope="projectFormula1-scope", key="SPADFValue"),
          "fs.azure.account.oauth2.client.endpoint": "https://login.microsoftonline.com/"+dbutils.secrets.get(scope="projectFormula1-scope", key="SPADF-DirID-TenantID")+"/oauth2/token"}

# COMMAND ----------

# Mounting ADLS To DBFS

#dbutils.fs.help()
dbutils.fs.mount(
  source = "abfss://data@dlgirishproject.dfs.core.windows.net/",
  mount_point = "/mnt/dlgirishproject/data",
  extra_configs = configs)

# COMMAND ----------

display(dbutils.fs.ls(dbutils.secrets.get(scope="projectFormula1-scope", key="InputFilePath")))
display(dbutils.fs.ls("/mnt/dlgirishproject/"))
dbutils.fs.unmount("/mnt/dlgirishproject/data")
#dbutils.fs.mounts()

# COMMAND ----------


