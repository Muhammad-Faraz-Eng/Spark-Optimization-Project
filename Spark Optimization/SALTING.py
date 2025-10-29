# Databricks notebook source
from pyspark.sql.functions import *

# COMMAND ----------

# MAGIC %md
# MAGIC Creating DataFrame

# COMMAND ----------

data = [("A", 100), ("A", 200), ("A", 300), ("B", 400), ("C", 500)]
df = spark.createDataFrame(data, ["User_Id", "Purchases"])

# COMMAND ----------

display(df)

# COMMAND ----------

# MAGIC %md
# MAGIC Adding Salt

# COMMAND ----------

df = df.withColumn("Salt_Col",floor(rand()*3))

# COMMAND ----------

display(df)

# COMMAND ----------

# MAGIC %md
# MAGIC Creating ConCat Column On GroupBy And Salt Column To Create A New Column

# COMMAND ----------

df = df.withColumn("User_Id_Column", concat(col("User_Id"),lit(" ") , col("Salt_Col")))
df.display()

# COMMAND ----------

# MAGIC %md
# MAGIC APPLY GROUPBY ON NEW COLUMN

# COMMAND ----------

df.groupBy("User_Id_Column").agg(sum("Purchases")).display()

# COMMAND ----------

