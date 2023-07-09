# Databricks notebook source
import os

# COMMAND ----------

dbutils.fs.rm('{}/raw/ukg/ukg_stf_matrx'.format(os.getenv("adls_file_path")),True)

# COMMAND ----------

drop_table = "DROP TABLE IF EXISTS ukg.ukg_stf_matrx"

create_table = """
CREATE TABLE `ukg`.`ukg_stf_matrx` (
  `RUN_ID` BIGINT,
  `STF_MATRX_ID` BIGINT,
  `STF_MATRX_NM` STRING,
  `STF_MATRX_DESCR` STRING,
  `ROW_INSERT_TSP` TIMESTAMP,
  `ROW_UPDT_TSP` TIMESTAMP,
  `INSERT_USER_ID` STRING,
  `UPDT_USER_ID` STRING
   )
USING delta
LOCATION '{}/raw/ukg/ukg_stf_matrx'""".format(os.getenv("adls_file_path"))

spark.sql(drop_table)
spark.sql(create_table)