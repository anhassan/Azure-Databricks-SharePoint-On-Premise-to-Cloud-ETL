# Databricks notebook source
import os

# COMMAND ----------

dbutils.fs.rm('{}/raw/ukg/ukg_wrkload_dtl'.format(os.getenv("adls_file_path")),True)

# COMMAND ----------

drop_table = "DROP TABLE IF EXISTS ukg.ukg_wrkload_dtl"

create_table = """
CREATE TABLE `ukg`.`ukg_wrkload_dtl` (
   `RUN_ID` BIGINT,
   `SCHDL_COVGE_SCHDLD_CNT` STRING,
  `SCHDL_WRKLOAD_PLND_CNT` STRING,
  `ORG_QLFR` STRING,
  `ORG_ID` BIGINT,
  `DAY_ID` DATE,
  `SCHDL_ZN_ID` INT,
  `SCHDL_ZN_QLFR` STRING,
  `ROW_INSERT_TSP` TIMESTAMP,
  `ROW_UPDT_TSP` TIMESTAMP,
  `INSERT_USER_ID` STRING,
  `UPDT_USER_ID` STRING
   )
USING delta
LOCATION '{}/raw/ukg/ukg_wrkload_dtl'""".format(os.getenv("adls_file_path"))

spark.sql(drop_table)
spark.sql(create_table)