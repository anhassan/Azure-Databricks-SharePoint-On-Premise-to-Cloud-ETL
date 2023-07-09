# Databricks notebook source
import os

# COMMAND ----------

dbutils.fs.rm('{}/raw/ukg/ukg_wrkload_dtl_attr'.format(os.getenv("adls_file_path")),True)

# COMMAND ----------

drop_table = "DROP TABLE IF EXISTS ukg.ukg_wrkload_dtl_attr"

create_table = """
CREATE TABLE `ukg`.`ukg_wrkload_dtl_attr` (
  `RUN_ID` BIGINT,
  `SCHDL_COVGE_SCHDLD_CNT` STRING,
  `SCHDL_WRKLOAD_PLND_CNT` STRING,
  `ORG_QLFR` STRING,
  `SCHDL_WRKLOAD_PLND_CNT_JOB` STRING,
  `SCHDL_WRKLOAD_PLND_CNT_DT` DATE,
  `SCHDL_WRKLOAD_PLND_CNT_SPAN_NM` STRING,
  `SCH_WRKLOAD_PLND_CNT` STRING,
  `SCHDL_COVGE_SCHDLD_CNT_JOB` STRING,
  `SCHDL_COVGE_SCHDLD_CNT_DT` DATE,
  `SCHDL_COVGE_SCHDLD_CNT_SPAN_NM` STRING,
  `SCHDL_COVGE_SCHDLD_CNT_SPAN_START_TM` STRING,
  `SCHDL_COVGE_SCHDLD_CNT_SPAN_END_TM` STRING,
  `SCH_COVGE_SCHDLD_CNT` STRING,
  `ROW_INSERT_TSP` TIMESTAMP,
  `ROW_UPDT_TSP` TIMESTAMP,
  `INSERT_USER_ID` STRING,
  `UPDT_USER_ID` STRING
   )
USING delta
LOCATION '{}/raw/ukg/ukg_wrkload_dtl_attr'""".format(os.getenv("adls_file_path"))

spark.sql(drop_table)
spark.sql(create_table)
