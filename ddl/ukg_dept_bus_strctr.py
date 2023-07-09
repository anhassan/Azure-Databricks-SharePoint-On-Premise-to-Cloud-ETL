# Databricks notebook source
import os

# COMMAND ----------

dbutils.fs.rm('{}/raw/ukg/ukg_dept_bus_strctr'.format(os.getenv("adls_file_path")),True)

# COMMAND ----------

drop_table = "DROP TABLE IF EXISTS ukg.ukg_dept_bus_strctr"

create_table = """
CREATE TABLE `ukg`.`ukg_dept_bus_strctr` (
  `RUN_ID` BIGINT,
  `FRCST_YN` STRING,
  `LOCTN_ABBR` STRING,
  `EPIC_DEPT_ID` BIGINT,
  `EPIC_DEPT_NM` STRING,
  `SMRT_SQ_UNIT_NM` STRING,
  `OLD_CST_CNTR` BIGINT,
  `CST_CNTR_ACCTNG_UNIT` STRING,
  `DEPT_BUS_STRCTR` STRING,
  `STF_MATRX` STRING,
  `NOTES` STRING,
  `ADC` STRING,
  `MAX_CPCTY` STRING,
  `MWOD_YES_NO` STRING,
  `STF_MATRX_YES_NO` STRING,
  `ROW_INSERT_TSP` TIMESTAMP,
  `ROW_UPDT_TSP` TIMESTAMP,
  `INSERT_USER_ID` STRING,
  `UPDT_USER_ID` STRING
   )
USING delta
LOCATION '{}/raw/ukg/ukg_dept_bus_strctr'""".format(os.getenv("adls_file_path"))

spark.sql(drop_table)
spark.sql(create_table)

# COMMAND ----------

os.getenv("adls_file_path")