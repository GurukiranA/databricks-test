# Databricks notebook source
from pyspark.sql.functions import max

# COMMAND ----------

file_location="dbfs:/FileStore/tables/datasets/"
file_type="csv"
infer_schema="true"
first_row_is_header="true"
delimiter=","

df=spark.read.format(file_type) \
    .option("inferSchema",infer_schema) \
    .option("header",first_row_is_header) \
    .option("sep",delimiter) \
    .load(file_location)

display(df)

# COMMAND ----------

df.select(max("Adj Close"), max ("Volume"))\
    .withColumnRenamed("max(Adj Close)","Max_Close")\
    .withColumnRenamed("max(Volume)","Max_Volume")\
    .show(truncate=False)

# COMMAND ----------
df.Select("Date","Adj Close","Volume")\
  .where(df.Volume > 150000000)\
  .write.option("header","true")\
        .mode('overwrite')\
        .mode('dbfs:/Filestore/outputs/tesla_highvoldays'.csv)

