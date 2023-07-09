# Databricks notebook source
# TYPE: PYTHON Commands
# DEFINITION: This notebook loads data from UKG Staffing Matrix API
# SCHEDULE: Daily Twice(9:10AM,3:10PM)
# 
# CHANGE HISTORY
# ---------------------------------------------------------------------------------------------------
# DATE           DEVELOPER         DESCRIPTION
# 01/01/2023     AHASSAN2          Initial creation

# COMMAND ----------

import json
import requests
import pyspark.sql.functions as F
from delta import DeltaTable
from pyspark.sql.types import StructType,StructField, StringType, IntegerType,DecimalType,DateType, TimestampType, LongType, FloatType
from decimal import Decimal,getcontext
from datetime import datetime
import pandas as pd
import os

# COMMAND ----------

# MAGIC %run /Shared/utils/env_var

# COMMAND ----------

ukg_dept_bus_strctr_qualifiers = spark.read.format("delta").table("ukg.ukg_dept_bus_strctr").agg(F.max(F.col("RUN_ID"))).distinct().toJSON().collect()
 
max_run_id =int(json.loads(ukg_dept_bus_strctr_qualifiers[0])['max(RUN_ID)'])

# COMMAND ----------

ukg_dept_bus_strctr_qualifiers = spark.read.format("delta").table("ukg.ukg_wrkload_dtl").agg(F.max(F.col("RUN_ID"))).distinct().toJSON().collect()


run_id =int(json.loads(ukg_dept_bus_strctr_qualifiers[0])['max(RUN_ID)'])

# COMMAND ----------

mapping_table_name = "ukg.ukg_dept_bus_strctr"

batch_size = 1

# COMMAND ----------

master_table_name = "ukg_stf_matrx"
child_table_name = "ukg_stf_matrx_range"

# COMMAND ----------

def get_access_token():
  token_url = access_token_url
  
  token_payload=access_token_payload
  token_headers = {
    'appkey': appkey,
    'Content-Type': 'application/x-www-form-urlencoded'
    }
  response = requests.request("POST", token_url, headers=token_headers, data=token_payload)
  return eval(response.text)["access_token"]

# COMMAND ----------

from pyspark.sql.functions import *

def get_valid_qualifiers(table_name):
  df = spark.sql("SELECT * FROM {}".format(table_name))
  df_valid_qualifiers = df.where(F.col("MWOD_YES_NO")=="yes")\
                          .where(F.col("STF_MATRX_YES_NO")=='YES')\
                          .where(F.col("RUN_ID")==max_run_id)\
                          .select(F.col("DEPT_BUS_STRCTR"))\
                         .distinct()
  valid_qualifiers = [qualifiers["DEPT_BUS_STRCTR"] for qualifiers in df_valid_qualifiers.collect()]
  return valid_qualifiers

# COMMAND ----------

import numpy as np
import math
import decimal

def generate_batchs(qualifiers,batch_size):
  num_splits = math.ceil(decimal.Decimal(len(qualifiers))/decimal.Decimal(batch_size))
  qualifiers_arr = np.array(qualifiers)
  qualifiers_arr_batchs = np.array_split(qualifiers_arr,num_splits)
  qualifiers_batchs = [list(batch) for batch in qualifiers_arr_batchs]
  return qualifiers_batchs
  
  

# COMMAND ----------

def get_workload_details_multi_read(auth_token,qualifiers):
  
  url = stfng_matrx_url
  
  
  payload = json.dumps({
    "where": {
     "locations": {
       "qualifiers": qualifiers
     }
    }
  })
  headers = {
  'Authorization': auth_token,
  'appkey': appkey,
  'Content-Type': 'application/json'
  }
  
  response = json.loads(requests.post(url,headers=headers,data=payload).text)
  return response

  

# COMMAND ----------


def get_master_child_df(response):
  master_table_cols = ["STF_MATRX_ID","STF_MATRX_NM","STF_MATRX_DESCR"]
  child_table_cols = ["STF_MATRX_ID","LOW_RANGE","HI_RANGE","STF_MATRX_ITEM_ID","STF_MATRX_ITEM_QLFR","STF_MATRX_SCHDL_ZN_ID","STF_MATRX_SCHDL_ZN_QLFR","STF_MATRX_COL_ID","STF_MATRX_COL_QLFR","COL_NBR","STF_CNT"]
  
  master_table_rows = []
  child_table_rows = []
  
  for element in response:
    master_table_keys = [element["id"],element["name"],element["description"]]
    master_table_rows += [master_table_keys]
    
#   for element in response:
    master_key = [element["id"]]
    for attribute in element["staffingMatrixRanges"]:
      child_table_keys = master_key + [attribute["lowRange"],attribute["highRange"]]
      for attribute in attribute["staffingMatrixItems"]:
          staff_cnt = {}
          if 'staffingCount' in attribute.keys():
            staff_cnt = attribute["staffingCount"]
          else:
            staff_cnt = 0
          sub_keys = child_table_keys + [attribute["item"]["id"],attribute["item"]["qualifier"],attribute["scheduleZone"]["id"],attribute["scheduleZone"]["qualifier"],attribute["column"]["id"],
                                        attribute["column"]["qualifier"],attribute["columnNumber"],staff_cnt]
        
          child_table_rows += [sub_keys]
     
  
  master_table_df = spark.createDataFrame(pd.DataFrame(master_table_rows,columns=master_table_cols))
  child_table_df = spark.createDataFrame(pd.DataFrame(child_table_rows,columns=child_table_cols))\
                   .withColumn("LOW_RANGE",col("LOW_RANGE").cast(DecimalType(5,1)))\
                   .withColumn("HI_RANGE",col("HI_RANGE").cast(DecimalType(5,1)))\
                   .withColumn("STF_CNT",col("STF_CNT").cast(DecimalType(5,1)))


  return master_table_df,child_table_df

# COMMAND ----------

def get_current_user():
  query = 'select current_user' 
  current_user = spark.sql(query)
  return current_user.collect()[0][0]

insert_user_id = get_current_user()
update_user_id = get_current_user()

# COMMAND ----------

def write_to_delta(df,table_name,mode="append"):
  df.withColumn("RUN_ID", abs(lit(run_id).cast(LongType())))\
    .withColumn("STF_MATRX_ID",col("STF_MATRX_ID").cast(LongType()))\
    .withColumn("ROW_INSERT_TSP", F.current_timestamp()) \
    .withColumn("ROW_UPDT_TSP", F.current_timestamp()) \
    .withColumn("INSERT_USER_ID",F.lit(insert_user_id)) \
    .withColumn("UPDT_USER_ID",F.lit(update_user_id)) \
    .write\
    .format("delta")\
    .mode(mode)\
    .save('{}/raw/ukg/{}'.format(os.getenv("adls_file_path"),table_name))

# COMMAND ----------

def load_to_delta(batch_size,mapping_table_name,master_table_name,child_table_name):
  
  master_dfs = []
  child_dfs = []
  
  auth_token = get_access_token()
  
  qualifiers = get_valid_qualifiers(mapping_table_name)
  qualifiers_batchs = generate_batchs(qualifiers,batch_size)
  
  for batch in qualifiers_batchs:
    try:
        response = get_workload_details_multi_read(auth_token,batch)
        master_df,child_df = get_master_child_df(response)
        master_dfs += [master_df]
        child_dfs += [child_df]
        
    except Exception as error:
        print(batch[0])
        print("Skipped Batch....")
        print("Reason for Skipping : {}".format(error))
    
  master_df_combined = master_dfs[0]
  child_df_combined = child_dfs[0]
  
  for index, master_df in enumerate(master_dfs):
    if index > 0:
      master_df_combined = master_df_combined.union(master_df)
      child_df_combined = child_df_combined.union(child_dfs[index])
      
  write_to_delta(master_df_combined,master_table_name,"append")
  write_to_delta(child_df_combined,child_table_name,"append")
  

# COMMAND ----------

load_to_delta(batch_size,mapping_table_name,master_table_name,child_table_name)