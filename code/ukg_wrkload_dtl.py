# Databricks notebook source
# TYPE: PYTHON Commands
# DEFINITION: This notebook loads data from UKG Workload Details API
# SCHEDULE: Daily Twice(9:10AM,3:10PM)
# 
# CHANGE HISTORY
# ---------------------------------------------------------------------------------------------------
# DATE           DEVELOPER         DESCRIPTION
# 01/01/2023     AHASSAN2          Initial creation


# COMMAND ----------

import requests
import json
from datetime import datetime
from datetime import timedelta
import time
import pyspark.sql.functions as F
import os


# COMMAND ----------

# MAGIC %run /Shared/utils/env_var

# COMMAND ----------

ukg_dept_bus_strctr_qualifiers = spark.read.format("delta").table("ukg.ukg_dept_bus_strctr").agg(F.max(F.col("RUN_ID"))).distinct().toJSON().collect()
 
max_run_id =int(json.loads(ukg_dept_bus_strctr_qualifiers[0])['max(RUN_ID)'])

# COMMAND ----------

mapping_table_name = "ukg.ukg_dept_bus_strctr"

workload_detail_keys = ["SCH_WORKLOAD_PLANNED_COUNT_JOB","SCH_WORKLOAD_PLANNED_COUNT_DATE","SCH_WORKLOAD_PLANNED_COUNT_SPAN_NAME","SCH_WORKLOAD_PLANNED_COUNT",
                       "SCH_COVERAGE_SCHEDULED_COUNT_JOB","SCH_COVERAGE_SCHEDULED_COUNT_DATE","SCH_COVERAGE_SCHEDULED_COUNT_SPAN_NAME","SCH_COVERAGE_SCHEDULED_COUNT_SPAN_START_TIME",
                       "SCH_COVERAGE_SCHEDULED_COUNT_SPAN_END_TIME","SCH_COVERAGE_SCHEDULED_COUNT"]
batch_size = 1


# COMMAND ----------

master_table_name = "ukg_wrkload_dtl"
child_table_name = "ukg_wrkload_dtl_attr"

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

def get_workload_details_multi_read(auth_token,workload_detail_keys,qualifiers,
                                   back_fill_days=0,date_range=None):
  
  url = wrkload_dtl_url
  
  if date_range is None or len(date_range) == 0:
    start_date = datetime.today().strftime('%Y-%m-%d')
    end_date = (datetime.today() - timedelta(days=back_fill_days)).strftime('%Y-%m-%d')
  else:
    if len(date_range) == 1:
      start_date = date_range[0]
      end_date = date_range[0]
    else:
      start_date = date_range[0]
      end_date = date_range[1]
  
  workload_details_dict = [{"key":key}for key in workload_detail_keys]
  
  payload = json.dumps({
    "select" : workload_details_dict,
    "from": {
    "view": 1,
    "locationSet": {
      "locations": {
        "qualifiers": qualifiers
        },
      "dateRange": {
        "startDate": start_date,
        "endDate": end_date
          }
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

import pandas as pd

def get_master_child_df(response,workload_detail_keys):
  
  master_table_cols = ["SCHDL_COVGE_SCHDLD_CNT","SCHDL_WRKLOAD_PLND_CNT","ORG_QLFR","ORG_ID","DAY_ID","SCHDL_ZN_ID","SCHDL_ZN_QLFR"]
  child_table_cols =  ["SCHDL_COVGE_SCHDLD_CNT","SCHDL_WRKLOAD_PLND_CNT","ORG_QLFR","SCHDL_WRKLOAD_PLND_CNT_JOB","SCHDL_WRKLOAD_PLND_CNT_DT","SCHDL_WRKLOAD_PLND_CNT_SPAN_NM",
                       "SCH_WRKLOAD_PLND_CNT","SCHDL_COVGE_SCHDLD_CNT_JOB","SCHDL_COVGE_SCHDLD_CNT_DT","SCHDL_COVGE_SCHDLD_CNT_SPAN_NM",
                       "SCHDL_COVGE_SCHDLD_CNT_SPAN_START_TM","SCHDL_COVGE_SCHDLD_CNT_SPAN_END_TM","SCH_COVGE_SCHDLD_CNT"]
  
  master_table_rows = []
  child_table_rows = []
  
  for element in response["data"]["children"]:
    
    master_child_table_keys = [element["key"]["SCHEDULE_COVERAGE_SCHEDULED_COUNT"], 
                              element["key"]["SCHEDULE_WORKLOAD_PLANNED_COUNT"],element["coreEntityKey"]["ORG"]["qualifier"]]

    master_table_rows += [master_child_table_keys + [element["coreEntityKey"]["ORG"]["id"],element["coreEntityKey"]["DAY"]["id"],
                         element["coreEntityKey"]["SCH_ZONE"]["id"],element["coreEntityKey"]["SCH_ZONE"]["qualifier"]]]
    
    attributes_dict = {}

    for attribute in element["attributes"]:
      attributes_dict[attribute["key"]] = attribute["value"]
    
    attributes_sorted = master_child_table_keys + [attributes_dict[workload_detail_key] for workload_detail_key in workload_detail_keys]
    child_table_rows += [attributes_sorted]

  master_table_df = spark.createDataFrame(pd.DataFrame(master_table_rows,columns=master_table_cols))\
                         .withColumn("ORG_ID",F.col("ORG_ID").cast("long"))\
                         .withColumn("DAY_ID",F.col("DAY_ID").cast("Date"))\
                         .withColumn("SCHDL_ZN_ID",F.col("SCHDL_ZN_ID").cast("int"))
  child_table_df = spark.createDataFrame(pd.DataFrame(child_table_rows,columns= child_table_cols))\
                        .withColumn("SCHDL_WRKLOAD_PLND_CNT_DT",to_date(F.col("SCHDL_WRKLOAD_PLND_CNT_DT"),"M/dd/yyyy"))\
                        .withColumn("SCHDL_COVGE_SCHDLD_CNT_DT",to_date(F.col("SCHDL_COVGE_SCHDLD_CNT_DT"),"M/dd/yyyy"))
    
  
  return master_table_df,child_table_df

    
  

# COMMAND ----------

def get_current_user():
  query = 'select current_user' 
  current_user = spark.sql(query)
  return current_user.collect()[0][0]

insert_user_id = get_current_user()
update_user_id = get_current_user()

# COMMAND ----------

def write_to_delta(df,table_name,mode="overwrite"):
  df.withColumn("RUN_ID", abs(lit(run_id).cast("long"))) \
    .withColumn("ROW_INSERT_TSP", current_timestamp()) \
    .withColumn("ROW_UPDT_TSP", current_timestamp()) \
    .withColumn("INSERT_USER_ID",lit(insert_user_id)) \
    .withColumn("UPDT_USER_ID",lit(update_user_id)) \
    .write\
    .format("delta")\
    .mode(mode)\
    .save('{}/raw/ukg/{}'.format(os.getenv("adls_file_path"),table_name))

# COMMAND ----------

def load_to_delta(workload_detail_keys,
                  batch_size,mapping_table_name,master_table_name,child_table_name,
                 back_fill_days=0,date_range=None):
  
  master_dfs = []
  child_dfs = []
  
  auth_token = get_access_token()
  
  qualifiers = get_valid_qualifiers(mapping_table_name)
  qualifiers_batchs = generate_batchs(qualifiers,batch_size)
  
  for batch in qualifiers_batchs:
    try:
        response = get_workload_details_multi_read(auth_token,workload_detail_keys,
                                                   batch,back_fill_days,date_range)
        master_df,child_df = get_master_child_df(response,workload_detail_keys)
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

# back_fill_days = int(dbutils.widgets.get("back_fill_days"))
# batch_size = int(dbutils.widgets.get("batch_size"))
# date_range = json.loads(dbutils.widgets.get("date_range"))
# run_id = int(time.time()*1000.0)

# COMMAND ----------

current_date = datetime.now()
back_fill_days = 0
batch_size = 1
date_range = []
run_id = (current_date.year * 1000000000000 +
      current_date.month * 10000000000 +
      current_date.day * 100000000 +
      current_date.hour * 1000000 +
      current_date.minute * 10000 +
      current_date.second * 100 + 
      int(str(current_date.microsecond)[:2]))

# COMMAND ----------

load_to_delta(workload_detail_keys,batch_size,mapping_table_name,master_table_name,child_table_name,back_fill_days,date_range)