# Databricks notebook source
# TYPE: PYTHON Commands
# DEFINITION: This notebook moves Department Business Structure file from Teams to ADLS. This notebook is referenced in ukg_dept_bus_strctr notebook.
# SCHEDULE: Daily Once(2:30PM)
# 
# CHANGE HISTORY
# ---------------------------------------------------------------------------------------------------
# DATE           DEVELOPER         DESCRIPTION
# 01/01/2023     AHASSAN2          Initial creation


# COMMAND ----------

# MAGIC %run /Shared/utils/env_var

# COMMAND ----------

tenant_id = file_tenant_id
client_id = file_client_id
client_secret = file_client_secret
site_id = file_site_id


# COMMAND ----------

share_point_doc = "Epic Integrations"
share_point_file = "Mercy_Epic_to_UKG_Forcaster_Mapping_Table.xlsx"
adls_dir_path = "UKG/Mapping"

# COMMAND ----------

import requests
import json

def get_auth_token_sharepoint(tenant_id,client_id,client_secret):
  url = "https://login.microsoftonline.com/{}/oauth2/v2.0/token".format(tenant_id)
  
  payload = 'grant_type=client_credentials&client_id={}&client_secret={}&scope=https%3A%2F%2Fgraph.microsoft.com%2F.default&Content-Type=application%2Fx-www-form-urlencoded'.format(client_id,client_secret)
  
  headers = {
      'Content-Type': 'application/x-www-form-urlencoded',
      'Cookie': 'fpc=Ang2-iYebXFEqfIG0EpCrmvgZA3vAgAAAGAW7toOAAAA; stsservicecookie=estsfd; x-ms-gateway-slice=estsfd'
  }
  
  response = json.loads(requests.post(url,headers=headers,data=payload).text)
  
  if "errorCode" in response.keys():
    raise Exception("Error : {}".format(response))
  else:
    return response["access_token"]


# COMMAND ----------

def get_item_id(auth_token,site_id,share_point_doc,share_point_file):
  
  headers = {'Authorization' : 'Bearer {}'.format(auth_token)}
  
  teams_channel_url = "https://graph.microsoft.com/v1.0/sites/{}/drive/root".format(site_id)
  teams_channel_id = json.loads(requests.get(teams_channel_url,headers=headers).text)["id"]

  doc_url = "https://graph.microsoft.com/v1.0/sites/{}/drive/items/{}/children".format(site_id,teams_channel_id)
  doc_id = [doc["id"] for doc in json.loads(requests.get(doc_url,headers=headers).text)["value"] if doc["name"] == share_point_doc][0]

  items_url = "https://graph.microsoft.com/v1.0/sites/{}/drive/items/{}/children".format(site_id,doc_id)
  items = json.loads(requests.get(items_url,headers=headers).text)["value"]
  item_id = [item["id"] for item in items if item["name"] == share_point_file][0]
  
  return item_id

# COMMAND ----------

def get_download_link(site_id,item_id,auth_token):
  url = "https://graph.microsoft.com/v1.0/sites/5c85c577-d44c-4164-8426-f07970334690/drive/items/{}/?select=id,@microsoft.graph.downloadUrl".format(item_id)
  headers = {
    'Authorization': 'Bearer {}'.format(auth_token)
  }
  response = json.loads(requests.get(url, headers=headers).text)
  return response['@microsoft.graph.downloadUrl']
  

# COMMAND ----------

from datetime import datetime

def get_last_modified_tm(auth_token,site_id,item_id):
  url = "https://graph.microsoft.com/v1.0/sites/{}/drive/items/{}?select=lastModifiedDateTime".format(site_id,item_id)
  headers = {
      'Authorization': 'Bearer {}'.format(auth_token)
    }
  response = json.loads(requests.get(url, headers=headers).text)
  file_last_modified_tm = response["lastModifiedDateTime"]
  last_modified_tm = datetime.strptime(file_last_modified_tm,"%Y-%m-%dT%H:%M:%SZ")
  return last_modified_tm

def write_to_delta(df,table_name,mode="overwrite"):
  df.write.format("delta").mode(mode).saveAsTable(table_name)


# COMMAND ----------

import pandas as pd

def persist_last_modified_tm(last_modified_tm):

  ingest_file = False
  df = spark.createDataFrame(pd.DataFrame([last_modified_tm],columns=["LastModifiedTimestamp"]))
  table_name = "ukg.teams_file_last_modified_tm"

  try:

    df = spark.sql("SELECT * FROM {}".format(table_name))
    prev_last_modified_tm = df.collect()[0]["LastModifiedTimestamp"]
    if last_modified_tm > prev_last_modified_tm:
      print("Ingesting File due to modifications.....")
      write_to_delta(df,table_name)
      ingest_file = True

  except Exception as error:
    print("Ingesting File for the first time......")
    write_to_delta(df,table_name)
    ingest_file = True
  
  return ingest_file



# COMMAND ----------

from urllib import request
import pandas as pd

def download_push_to_adls(download_url,share_point_file,adls_dir_path,
                          src_file_type="xls",dst_file_type="csv"):
  
  share_point_file = share_point_file[:share_point_file.find(".")]
  src_filename = "{}".format(share_point_file)
  df = pd.read_excel(download_url)
  df.to_csv("/dbfs/mnt/datalake/{}/{}.{}".format(adls_dir_path,src_filename,dst_file_type),
           sep=",",header=True,index=False)
  print("Uploaded {}.{} to Adls".format(src_filename,dst_file_type))

# COMMAND ----------

# from urllib import request
# import pandas as pd

# def download_push_to_adls(download_url,share_point_file,adls_dir_path,
#                           src_file_type="xls",dst_file_type="csv"):
  
#   share_point_file = share_point_file[:share_point_file.find(".")]
#   src_filename = "{}.{}".format(share_point_file,src_file_type)
#   response = request.urlretrieve(download_url,src_filename)
#   df = pd.read_excel(src_filename,engine="openpyxl")
# #   print("Downloaded {} from Sharepoint".format(src_filename))
#   df.to_csv("/dbfs/mnt/datalake/{}/{}.{}".format(adls_dir_path,share_point_file,dst_file_type),
#            sep=",",header=True,index=False)
#   print("Uploaded {}.{} to Adls".format(share_point_file,dst_file_type))
  

# COMMAND ----------

!pip install openpyxl


# COMMAND ----------

auth_token = get_auth_token_sharepoint(tenant_id,client_id,client_secret)
item_id = get_item_id(auth_token,site_id,share_point_doc,share_point_file)
last_modified_tm = get_last_modified_tm(auth_token,site_id,item_id)
ingest_file = persist_last_modified_tm(last_modified_tm)
if ingest_file:
  download_link = get_download_link(site_id,item_id,auth_token)
  download_push_to_adls(download_link,share_point_file,adls_dir_path)