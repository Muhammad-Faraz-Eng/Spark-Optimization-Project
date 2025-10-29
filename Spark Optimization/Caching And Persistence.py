# Databricks notebook source
from pyspark.sql.functions import *
from pyspark.storagelevel import StorageLevel

# COMMAND ----------

df = spark.read.format("csv")\
    .option('inferschema', 'true')\
    .option('header', 'true')\
    .load("/FileStore/data/BigMart_Sales.csv")\
    .cache()

# COMMAND ----------

df2 = df.filter(col("Outlet_Location_Type") == 'Tier 1')

# COMMAND ----------

df3 = df.filter(col("Outlet_Location_Type") == 'Tier 2')

# COMMAND ----------

display(df3)

# COMMAND ----------

df.persist(StorageLevel.MEMORY_ONLY)

# COMMAND ----------

