# Databricks notebook source
configs = {"fs.azure.account.auth.type": "OAuth",
           "fs.azure.account.oauth.provider.type": "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider",
           "fs.azure.account.oauth2.client.id": "a503e04e-a07f-4f6c-8b96-0712b6d32e9a",
           "fs.azure.account.oauth2.client.secret": dbutils.secrets.get(scope="dottscope",key="spnsecret"),
           "fs.azure.account.oauth2.client.endpoint": "https://login.microsoftonline.com/9a551814-37bd-44d1-9d94-07fd707bcdb3/oauth2/token"}

# COMMAND ----------

dbutils.fs.mount(
  source = "abfss://inputs@dottasmnt01.dfs.core.windows.net/",
  mount_point = "/mnt/inputs",
  extra_configs = configs)

# COMMAND ----------

dbutils.fs.mount(
  source = "abfss://outputs@dottasmnt01.dfs.core.windows.net/",
  mount_point = "/mnt/outputs",
  extra_configs = configs)

# COMMAND ----------

spark.sql("create database _source")
spark.sql("create database _results")

# COMMAND ----------

dbutils.fs.ls("/mnt")
