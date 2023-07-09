# Databricks notebook source
# TYPE: PYTHON Commands
# DEFINITION: This notebook clean up RWB Open census file and gets latest file from ADLS
# 
# CHANGE HISTORY
# ---------------------------------------------------------------------------------------------------
# DATE           DEVELOPER         DESCRIPTION
# 01/01/2023     AHASSAN2          Initial creation
# 03/08/2023     AHASSAN2          Fixed get_unprocessed_files function to bring distinct file names in select clause to improve performance 



# COMMAND ----------

# MAGIC %run /Shared/utils/env_var

# COMMAND ----------

import os
from pyspark.sql.functions import *
import pytz
from azure.storage.filedatalake import DataLakeServiceClient
import pandas as pd
import re

# COMMAND ----------

census_data_dir_path = "{}/{}".format(os.getenv("adls_file_path"),"UKG/RWB/Census")
census_table_name = "epic_rltm.rwb_currnt_admt_ptnt"

# COMMAND ----------

def get_unprocessed_files(dir_path, table_name):
  
  try:
    df = spark.sql(" SELECT distinct file_nm FROM {}".format(table_name))
    already_processed_files = list(set([row["file_nm"] for row in df.collect()]))
  except Exception as error:
    print("Error : {}".format(error))
    already_processed_files = []
    
  df_new = spark.read\
                .format("csv")\
                .option("header","True")\
                .load(dir_path)\
                .withColumn("input_file_path",input_file_name())\
                .withColumn("input_file_name",element_at(split(col("input_file_path"),"/"),-1))

  current_files = list(set([row["input_file_name"] for row in df_new.collect()]))
  new_files = [file for file in current_files if file not in already_processed_files]
  return new_files

# COMMAND ----------

def preprocess_data(file_name,dir_path="UKG/RWB/Census",container_name="idp-datalake"):
  service_client = DataLakeServiceClient(account_url = "https://{}.dfs.core.windows.net".format(storage_account_name),credential=access_key)
  container_client = service_client.get_file_system_client(file_system=container_name)
  directory_client = container_client.get_directory_client(dir_path)
  file_client = directory_client.get_file_client(file_name)
  download=file_client.download_file()
  downloaded_bytes = download.readall()
  cleansed_data = str.encode(re.sub("(?<!\r)\n", " ", downloaded_bytes.decode("utf-8") ))
  file_client.upload_data(cleansed_data, overwrite=True)

# COMMAND ----------

import csv

def preprocess_binary_data(file_name,dir_path="UKG/Binary/RWB/Census",container_name="idp-datalake"):

  service_client = DataLakeServiceClient(account_url = "https://{}.dfs.core.windows.net".format(storage_account_name),credential=access_key)
  container_client = service_client.get_file_system_client(file_system=container_name)
  directory_client = container_client.get_directory_client(dir_path)
  file_client = directory_client.get_file_client(file_name)

  download=file_client.download_file()
  downloaded_bytes = download.readall()

  all_rows = []
  
  try:
    csv_reader = csv.reader(downloaded_bytes.decode("utf-8").splitlines(),delimiter=",")
  except:
    csv_reader = csv.reader(downloaded_bytes.decode("cp1252").splitlines(),delimiter=",")

  for row in csv_reader:
      new_row = ",".join([element.replace('"',"").replace(",","") for element in row])
      all_rows.append(new_row)
  
  if len(all_rows) > 1:
    cleansed_content = "\n".join(all_rows[1:])

    file_client.upload_data(cleansed_content, overwrite=True)
  