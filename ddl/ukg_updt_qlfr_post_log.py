# Databricks notebook source
import os

# COMMAND ----------

dbutils.fs.rm('{}/raw/ukg/ukg_updt_qlfr_post_log'.format(os.getenv("adls_file_path")),True)

# COMMAND ----------

drop_table = "DROP TABLE IF EXISTS ukg.ukg_updt_qlfr_post_log"

create_table = """
CREATE TABLE `ukg`.`ukg_updt_qlfr_post_log` (
  `QLFR_DESCRPTR` STRING,
  `STAT_CD` BIGINT,
  `RESP_TXT` STRING,
  `ROW_INSERT_TSP` TIMESTAMP
   )
USING delta
LOCATION '{}/raw/ukg/ukg_updt_qlfr_post_log'""".format(os.getenv("adls_file_path"))

spark.sql(drop_table)
spark.sql(create_table)