# Databricks notebook source
import os

# COMMAND ----------

dbutils.fs.rm('{}/raw/ukg/ukg_open_census_post_data'.format(os.getenv("adls_file_path")),True)

# COMMAND ----------

drop_table = "DROP TABLE IF EXISTS ukg.ukg_open_census_post_data"

create_table = """
CREATE TABLE `ukg`.`ukg_open_census_post_data` (
  `DEPT_BUS_STRCTR` STRING,
  `CENSUS_DTTM` TIMESTAMP,
  `CENSUS_CNT` BIGINT,
  `SCHDL_ZN_NM` STRING,
  `SCHDL_ZN_START_TM` STRING,
  `SCHDL_ZN_END_TM` STRING,
  `EFF_DT` TIMESTAMP,
  `NEXT_SCHDL_ZN_DTTM` STRING,
  `NEXT_SCHDL_ZN_NM` STRING,
  `FILE_PRCSD` STRING
   )
USING delta
LOCATION '{}/raw/ukg/ukg_open_census_post_data'""".format(os.getenv("adls_file_path"))

spark.sql(drop_table)
spark.sql(create_table)