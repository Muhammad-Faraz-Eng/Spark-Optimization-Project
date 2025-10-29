# Databricks notebook source
from pyspark.sql.functions import *
spark.conf.set("spark.sql.adeptive.enabled", "false")

# COMMAND ----------

# Big Dataframe

df_transcation = spark.createDataFrame([
    (1 , "UK", 100),
    (2 , "US", 200),
    (3 , "TUR", 300),
    (4 , "PAK", 500)
], ["ID","Country_Code","Amount"])


#Small DataFrame

df_countries = spark.createDataFrame([
    ("UK", "United Kingdom"),
    ("US", "United States"),
    ("TUR", "Turkey"),
    ("PAK", "Pakistan")
] , ["Country_Code" , "Country_Name"])

# COMMAND ----------

display(df_transcation)
display(df_countries)

# COMMAND ----------

df_join = df_transcation.join(df_countries,df_transcation["Country_Code"]==df_countries["Country_Code"], "inner")

# COMMAND ----------

display(df_join)

# COMMAND ----------

df_join_opt = df_transcation.join(broadcast(df_countries),df_transcation["Country_Code"]==df_countries["Country_Code"], "inner")

# COMMAND ----------

display(df_join_opt)

# COMMAND ----------

