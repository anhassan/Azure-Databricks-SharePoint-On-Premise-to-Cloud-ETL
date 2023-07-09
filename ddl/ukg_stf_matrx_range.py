# Databricks notebook source
import os

# COMMAND ----------

dbutils.fs.rm('{}/raw/ukg/ukg_stf_matrx_range'.format(os.getenv("adls_file_path")),True)

# COMMAND ----------

drop_table = "DROP TABLE IF EXISTS ukg.ukg_stf_matrx_range"

create_table = """
CREATE TABLE `ukg`.`ukg_stf_matrx_range` (
  `RUN_ID` BIGINT,
  `STF_MATRX_ID` BIGINT,
  `LOW_RANGE` DECIMAL(5,1),
  `HI_RANGE` DECIMAL(5,1),
  `STF_MATRX_ITEM_ID` BIGINT,
  `STF_MATRX_ITEM_QLFR` STRING,
  `STF_MATRX_SCHDL_ZN_ID` BIGINT,
  `STF_MATRX_SCHDL_ZN_QLFR` STRING,
  `STF_MATRX_COL_ID` BIGINT,
  `STF_MATRX_COL_QLFR` STRING,
  `COL_NBR` BIGINT,
  `STF_CNT` DECIMAL(5,1),
  `ROW_INSERT_TSP` TIMESTAMP,
  `ROW_UPDT_TSP` TIMESTAMP,
  `INSERT_USER_ID` STRING,
  `UPDT_USER_ID` STRING
   )
USING delta
LOCATION '{}/raw/ukg/ukg_stf_matrx_range'""".format(os.getenv("adls_file_path"))

spark.sql(drop_table)
spark.sql(create_table)