# Databricks notebook source
# MAGIC %md
# MAGIC TURN OFF AQE

# COMMAND ----------

spark.conf.set("spark.sql.adaptive.enabled", "false")

# COMMAND ----------

spark.conf.get("spark.sql.adaptive.enabled")

# COMMAND ----------

from pyspark.sql.functions import *

# COMMAND ----------

# MAGIC %md
# MAGIC READING DATA

# COMMAND ----------

df= spark.read.format("csv")\
    .option('header', 'true')\
    .option('inferschema', 'true')\
    .load("/FileStore/data/BigMart_Sales.csv")

# COMMAND ----------

display(df)

# COMMAND ----------

# MAGIC %md
# MAGIC CHANGING DEFAULT PARTITION SIZE TO 128KB

# COMMAND ----------

spark.conf.set("spark.sql.files.maxPartitionBytes", 131072)

# COMMAND ----------

# MAGIC %md
# MAGIC GET NUMBER OF PARTITIONS

# COMMAND ----------

df.rdd.getNumPartitions()

# COMMAND ----------

# MAGIC %md
# MAGIC CHANGING DEFAULT PARTITION SIZE TO 128MB BACK

# COMMAND ----------

spark.conf.set("spark.sql.files.maxPartitionBytes", 134218828)

# COMMAND ----------

# MAGIC %md
# MAGIC REPARTITION

# COMMAND ----------

df = df.repartition(10)

# COMMAND ----------

df.rdd.getNumPartitions()

# COMMAND ----------

# MAGIC %md
# MAGIC GET PARTITION INFO

# COMMAND ----------

df.withColumn("Partition Info", spark_partition_id()).display()

# COMMAND ----------

# MAGIC %md
# MAGIC READ DATA

# COMMAND ----------

df.write.format('parquet')\
  .mode('append')\
  .option("path", "/FileStore/data/ParquetWrite")\
  .save()

# COMMAND ----------

# MAGIC %md
# MAGIC NEW DATA READ

# COMMAND ----------

df_new = spark.read.format('parquet')\
  .load("/FileStore/data/ParquetWrite")

df_new = df_new.filter(col("Outlet_Location_Type") ==  'Tier 1')

# COMMAND ----------

display(df_new)

# COMMAND ----------

# MAGIC %md
# MAGIC SCAN OPTIMIZATION

# COMMAND ----------

df.write.format('parquet')\
  .mode('append')\
  .partitionBy("Outlet_Location_Type")\
  .option("path", "/FileStore/data/ParquetWriteOpt")\
  .save()

# COMMAND ----------

df_new = spark.read.format('parquet')\
  .load("/FileStore/data/ParquetWriteOpt")

df_new = df_new.filter(col("Outlet_Location_Type") ==  'Tier 1')

display(df_new)

# COMMAND ----------

# MAGIC %md
# MAGIC JOIN OPTIMIZATION

# COMMAND ----------

