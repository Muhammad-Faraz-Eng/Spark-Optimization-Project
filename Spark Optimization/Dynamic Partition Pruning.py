# Databricks notebook source
from pyspark.sql.functions import *

# COMMAND ----------

spark.conf.set("spark.sql.adeptive.enabled", "false")
spark.conf.set("spark.sql.optimizer.dynamicPartitionPruning.enabled", "false")
spark.conf.set("spark.sql.autoBroadcastJoinThreshold", -1)

# COMMAND ----------

df= spark.read.format("csv")\
    .option('header', 'true')\
    .option('inferschema', 'true')\
    .load("/FileStore/data/BigMart_Sales.csv")

df = df.limit(8)

# COMMAND ----------

display(df)

# COMMAND ----------

# MAGIC %md
# MAGIC **Prepare The Partition Data**

# COMMAND ----------

df.write.format('parquet')\
    .mode('append')\
    .partitionBy("Outlet_Type")\
    .option("path", "/FileStore/data/pdd_Partition")\
    .save()

# COMMAND ----------

# MAGIC %md
# MAGIC **Prepare The Non-Partition Data**

# COMMAND ----------

df.write.format('parquet')\
    .mode('append')\
    .option("path", "/FileStore/data/pdd_nonPartition")\
    .save()

# COMMAND ----------

# MAGIC %md
# MAGIC **DataFrames**

# COMMAND ----------

df1 = spark.read.format("parquet")\
  .load("/FileStore/data/pdd_Partition")

# COMMAND ----------

df2 = spark.read.format("parquet")\
  .load("/FileStore/data/pdd_nonPartition")

# COMMAND ----------

# MAGIC %md
# MAGIC **JOINS**

# COMMAND ----------

df_joins = df1.join(df2.filter(col("Item_Identifier") == "FDA15"),df1["Item_Identifier"]==df2["Item_Identifier"], "inner")

# COMMAND ----------

display(df_joins)

# COMMAND ----------

