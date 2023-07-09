# Databricks notebook source
# TYPE: PYTHON Commands
# DEFINITION: This notebook loads UKG Department Business Structures
# SCHEDULE: Daily Once(2:30PM)
# 
# CHANGE HISTORY
# ---------------------------------------------------------------------------------------------------
# DATE           DEVELOPER         DESCRIPTION
# 01/01/2023     AHASSAN2       Initial creation


# COMMAND ----------

# MAGIC %run ./download_from_sharepoint

# COMMAND ----------

from pyspark.sql.functions import *
from datetime import datetime, timezone, date, timedelta
from pyspark.sql.types import *
import requests
import json,time
import os
import numpy as np

# COMMAND ----------

def get_row_time():
  return datetime.now(timezone.utc)

def get_current_user():
  query = 'select current_user' 
  current_user = spark.sql(query)
  return current_user.collect()[0][0]

def drop_na(df,cols=["epic_dept_id", "DEPT_BUS_STRCTR"]):
  for column in cols:
    df = df.where(col(column).isNotNull())
  return df

insert_user_id = get_current_user()
update_user_id = get_current_user()

# COMMAND ----------

mapping_schema = StructType(
	[
		StructField("FRCST_YN", StringType(), True),
		StructField("LOCTN_ABBR", StringType(), True),
		StructField("EPIC_DEPT_ID", StringType(), True),
		StructField("EPIC_DEPT_NM", StringType(), True),
		StructField("SMRT_SQ_UNIT_NM", StringType(), True),
		StructField("OLD_CST_CNTR", StringType(), True),
		StructField("CST_CNTR_ACCTNG_UNIT", StringType(), True),
		StructField("DEPT_BUS_STRCTR", StringType(), True),
		StructField("STF_MATRX", StringType(), True),
		StructField("NOTES", StringType(), True),
		StructField("ADC", StringType(), True),
		StructField("MAX_CPCTY", StringType(), True),
		StructField("MWOD_YES_NO", StringType(), True),
  		StructField("STF_MATRX_YES_NO", StringType(), True)	
	]
)

# COMMAND ----------

def write_mapping_data(file_path, curr_run_id):
  
  current_user = get_current_user()
#   curr_dt = datetime.now()
  
  mapping_df = spark.read.format("org.apache.spark.sql.execution.datasources.csv.CSVFileFormat").option("inferSchema", "false").option("header","true").schema(mapping_schema).load(file_path)
  
  drop_na(mapping_df.withColumn("EPIC_DEPT_ID",col("EPIC_DEPT_ID").cast("long"))\
    .withColumn("OLD_CST_CNTR",col("OLD_CST_CNTR").cast("long"))\
    .withColumn("RUN_ID",abs(lit(curr_run_id).cast("long"))) \
    .withColumn("ROW_INSERT_TSP", current_timestamp()) \
    .withColumn("ROW_UPDT_TSP", current_timestamp()) \
    .withColumn("INSERT_USER_ID",lit(insert_user_id)) \
    .withColumn("UPDT_USER_ID",lit(update_user_id)))\
    .write.format("delta").mode("append").saveAsTable("ukg.ukg_dept_bus_strctr")

# COMMAND ----------

import datetime
current_date  = datetime.datetime.now()
mapping_table_name = "ukg.ukg_dept_bus_strctr"
mapping_data_dir_path = "{}/{}".format(os.getenv("adls_file_path"),"UKG/Mapping")
mapping_data_file_name = "Mercy_Epic_to_UKG_Forcaster_Mapping_Table.csv"

# COMMAND ----------

RUN_ID = (current_date.year * 1000000000000 +
      current_date.month * 10000000000 +
      current_date.day * 100000000 +
      current_date.hour * 1000000 +
      current_date.minute * 10000 +
      current_date.second * 100 + 
      int(str(current_date.microsecond)[:2]))

  
write_mapping_data("{}/{}".format(mapping_data_dir_path,mapping_data_file_name), RUN_ID)
