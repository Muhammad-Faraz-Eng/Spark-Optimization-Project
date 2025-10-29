# Databricks notebook source
# MAGIC %md
# MAGIC **TRUN OFF AQE**

# COMMAND ----------

spark.conf.set("spark.sql.adaptive.enabled", "true")

# COMMAND ----------

from pyspark.sql.functions import *

# COMMAND ----------

df= spark.read.format("csv")\
    .option('header', 'true')\
    .option('inferschema', 'true')\
    .load("/FileStore/data/BigMart_Sales.csv")

# COMMAND ----------

df.rdd.getNumPartitions()

# COMMAND ----------

df_new = df.groupBy("Item_fat_Content").count()

display(df_new)

# COMMAND ----------

