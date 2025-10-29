# Databricks notebook source
# Big Dataframe

df = spark.createDataFrame([
    (1001, ),
    (1002, ),
    (1003, )
], ["Product_Id"])


#Small DataFrame

product_Dict = {
    "1001": "Iphone",
    "1002": "Samsung",
    "1003": "Oppo"
}

# COMMAND ----------

display(df)

# COMMAND ----------

broad_vr = spark.sparkContext.broadcast(product_Dict)

# COMMAND ----------

broad_vr.value

# COMMAND ----------

def mymap(x):
    return broad_vr.value.get(x)

# COMMAND ----------

mymap_udf = udf(mymap)

# COMMAND ----------

df_with_names = df.withColumn("Product_Name",mymap_udf("Product_Id"))

# COMMAND ----------

display(df_with_names)

# COMMAND ----------

