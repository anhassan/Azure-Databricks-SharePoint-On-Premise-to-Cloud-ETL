# Databricks notebook source
import os

table_name = "ukg_dept_bus_strctr"
destination_parquet_path = "{}/raw/{}_parquet".format(os.getenv("adls_file_path"),table_name)

# COMMAND ----------

ukg_delta = spark.sql("SELECT * FROM ukg.{} WHERE run_id = (SELECT MAX(RUN_ID) FROM ukg.{})".format(table_name,table_name))

# COMMAND ----------

ukg_delta.write.parquet(path=destination_parquet_path,mode="overwrite")