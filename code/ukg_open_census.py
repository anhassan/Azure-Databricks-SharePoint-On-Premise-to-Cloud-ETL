# Databricks notebook source
# TYPE: PYTHON Commands
# DEFINITION: This notebook loads OpenCensus Reporting Workbench files and posts data to API
# 
# CHANGE HISTORY
# ---------------------------------------------------------------------------------------------------
# DATE           DEVELOPER         DESCRIPTION
# 01/01/2023     AHASSAN2          Initial creation
# 04/12/2023     AHASSA2           Updated the logic to incorporate rolling window of variable size (one actual and multiple plans)

# COMMAND ----------

# MAGIC %run /Shared/utils/env_var

# COMMAND ----------

# MAGIC %run ./ukg_open_census_file_cleanser

# COMMAND ----------

#from pyspark.sql.functions import *
from datetime import datetime, timezone, date, timedelta
from pyspark.sql.types import *
import requests
import json,time
import ntpath
from dateutil import tz
from pyspark.sql.window import Window
from pyspark.sql.functions import row_number


# COMMAND ----------

def get_row_time():
  return datetime.now(timezone.utc)

def get_current_user():
  query = 'select current_user'
  current_user = spark.sql(query)
  return current_user.collect()[0][0]

def drop_na(df,cols=["epic_dept_id", "dept_bus_strctr"]):
  for column in cols:
    df = df.where(col(column).isNotNull())
  return df

def path_leaf(path):
    head, tail = ntpath.split(path)
    return tail or ntpath.basename(head)



# COMMAND ----------

ukg_dept_bus_strctr_qualifiers = spark.read.format("delta").table("ukg.ukg_dept_bus_strctr").agg(max(col("RUN_ID"))).distinct().toJSON().collect()
 
max_run_id =int(json.loads(ukg_dept_bus_strctr_qualifiers[0])['max(RUN_ID)'])

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

def to_date(date_str,date_format="%Y-%m-%d"):
  return datetime.strptime(date_str,date_format)

def to_datetime(datetime_str,datetime_format="%Y-%m-%d %H:%M:%S"):
  return datetime.strptime(datetime_str,datetime_format)

def add_days(curr_date,num_days):
  new_date = curr_date + timedelta(days=num_days)
  return new_date

# COMMAND ----------

  def get_scheduled_zones_json(current_date,qualifiers):
    zones_url = schdl_zones_url
    zones_payload = json.dumps({
      "where": {
        "effectiveDate": current_date,
        "expirationDate": current_date,
        "locations": {
          "qualifiers": qualifiers
          }
        }
      })

    zones_headers = {
      'appkey': appkey,
      'Authorization': get_access_token(),
      'Content-Type': 'application/json'
    }

    response = requests.request("POST", zones_url, headers=zones_headers, data=zones_payload)
    if response.status_code == 200:
      return json.loads(response.text)
    else:
      raise Exception("Failed getting scheduled zones for qualifiers : {} on date : {} ".format(qualifiers,date))


  def parse_schedule_zones(current_date,data,rolling_window):
    all_parsed_data =  [[item["location"]["qualifier"],
                         item["expirationDate"],
                         item["scheduleZoneSet"]["scheduleZones"]]
                         for item in data]
    expired_qualifiers = [item[0] for item in all_parsed_data if to_date(item[1]) < add_days(to_date(current_date),rolling_window)]
    return expired_qualifiers,[item for item in all_parsed_data if to_date(item[1]) >= add_days(to_date(current_date),rolling_window) ]
    

  def get_scheduled_zones(current_date,qualifiers,rolling_window):
    response = get_scheduled_zones_json(current_date,qualifiers)
    expired_qualifiers,schedule_zones = parse_schedule_zones(current_date,response,rolling_window)
    return expired_qualifiers,schedule_zones

  def get_scheduled_zones_cost_centers(current_date,invalid_qualifiers,rolling_window,precision=4):
    cost_centers = ["/".join(qualifier.split("/")[0:precision]) for qualifier in invalid_qualifiers]
    expired_cost_centers,cost_centers_schedule_zones = get_scheduled_zones(current_date,cost_centers,rolling_window)
    return expired_cost_centers,cost_centers_schedule_zones




# COMMAND ----------

import pandas as pd
from pyspark.sql.functions import col, asc,desc

def explode_schedule_zones(current_date,rolling_window,schedule_zones_info):
  
  flat_schedule_zones = []
  exploded_schedule_zones = []
  column_names = ["effectiveDate","expirationDate","location","name","startTime","endTime"]

  for qualifier,expiration_date,schedules in schedule_zones_info:
    for schedule in schedules:
      start_time = to_datetime("{} {}".format(current_date,schedule["startTime"]))
      end_time = to_datetime("{} {}".format(current_date,schedule["endTime"]))
      if start_time > end_time:
        end_time = add_days(end_time,1)
      element = [to_date(current_date),to_date(expiration_date),qualifier,schedule["name"],start_time,end_time]
      flat_schedule_zones.append(element)

  for item in flat_schedule_zones:
    for num_day in range(rolling_window+1):
      exploded_schedule_zones.append([add_days(item[0],num_day),add_days(item[1],num_day),item[2],
                                     item[3],add_days(item[4],num_day),add_days(item[5],num_day)])

  df = pd.DataFrame(exploded_schedule_zones,columns=column_names)
  schedule_zones_exploded = spark.createDataFrame(df).orderBy(col("location"),col("startTime").asc())
  return schedule_zones_exploded
  

# COMMAND ----------

def get_schedule_zones(qualifiers,current_date,rolling_window):
  
  expired_qualifiers,schedule_zones_info = get_scheduled_zones(current_date,qualifiers,rolling_window)

  valid_qualifiers = [item[0] for item in schedule_zones_info]
  invalid_qualifiers = [qualifier for qualifier in qualifiers if qualifier not in valid_qualifiers ]

  expired_cost_centers,cost_centers_schedule_zones_info = get_scheduled_zones_cost_centers(current_date,invalid_qualifiers,rolling_window)

  all_schedule_zones = schedule_zones_info + cost_centers_schedule_zones_info
  all_expired = expired_qualifiers + expired_cost_centers

  schedule_zones_exploded = explode_schedule_zones(current_date,rolling_window,all_schedule_zones)
  return all_expired, schedule_zones_exploded
  

# COMMAND ----------

def get_cost_center(location):
  return "/".join(location.split("/")[:4])

get_cost_center_udf = udf(get_cost_center, StringType())

# COMMAND ----------

def get_cost_center_5(location):
  return "/".join(location.split("/")[:5])

get_cost_center_5_udf = udf(get_cost_center_5, StringType())

# COMMAND ----------

census_schema = StructType(
	[
		StructField("pat_enc_csn_id", StringType(), True), 
		StructField("pat_mrn_id", StringType(), True), 
		StructField("ptnt_first_nm", StringType(), True), 
		StructField("ptnt_middle_nm", StringType(), True), 
		StructField("ptnt_last_nm", StringType(), True), 
		StructField("birth_date", StringType(), True), 
		StructField("age", StringType(), True), 
		StructField("death_date", StringType(), True), 
		StructField("ptnt_class", StringType(), True), 
		StructField("loctn_nm", StringType(), True), 
		StructField("dept_nm", StringType(), True), 
		StructField("room_nm", StringType(), True), 
		StructField("bed_label", StringType(), True), 
		StructField("arrival_dttm", StringType(), True), 
		StructField("bed_reqst_dttm", StringType(), True), 
		StructField("bed_reqst_to_bed_asgnd", StringType(), True), 
		StructField("bed_reqst_to_bed_ready", StringType(), True), 
		StructField("bed_reqst_to_depart_ed", StringType(), True),
		StructField("depart_from_ed_dttm", StringType(), True), 
		StructField("admsn_dttm", StringType(), True), 
		StructField("ed_disposition", StringType(), True), 
		StructField("ed_los", StringType(), True), 
		StructField("admtng_pvdr_nm", StringType(), True), 
		StructField("admtng_pvdr_id", StringType(), True), 
		StructField("acuity_level", StringType(), True), 
		StructField("accommodation_cd", StringType(), True), 
		StructField("lvl_of_care", StringType(), True), 
		StructField("dschrg_ord_stat", StringType(), True), 
		StructField("o2_dvc", StringType(), True), 
		StructField("cd_stat", StringType(), True), 
		StructField("dschrg_dttm", StringType(), True), 
		StructField("dschrg_disposition", StringType(), True), 
		StructField("dschrg_pvdr_nm", StringType(), True), 
		StructField("dschrg_pvdr_id", StringType(), True), 
		StructField("prmry_payor", StringType(), True), 
		StructField("prmry_plan", StringType(), True), 
		StructField("pcp_pvdr_id", StringType(), True), 
		StructField("pcp_pvdr_nm", StringType(), True), 
		StructField("attndng_pvdr_nm", StringType(), True), 
		StructField("tm_since_lst_dschrg_ord", StringType(), True), 
		StructField("pthwy_not_appld", StringType(), True), 
		StructField("encntr_dept_id", StringType(), True)
	]
)


# COMMAND ----------

def write_census_data(file_path):
  
  census_df = spark.read.format("org.apache.spark.sql.execution.datasources.csv.CSVFileFormat").option("inferSchema", "false").option("header","true").schema(census_schema).load(file_path)
  current_user = get_current_user()
  curr_dt = datetime.now()

  file_name = path_leaf(file_path)
  file_mod_time = get_adls_file_timestamp(file_name)
        
  census_df.withColumn("pat_enc_csn_id", col("pat_enc_csn_id").cast(DecimalType(18, 0))) \
    .withColumn("bed_reqst_to_bed_asgnd", col("bed_reqst_to_bed_asgnd").cast(DecimalType(10, 0))) \
    .withColumn("bed_reqst_to_bed_ready", col("bed_reqst_to_bed_ready").cast(DecimalType(10, 0))) \
    .withColumn("bed_reqst_to_depart_ed", col("bed_reqst_to_depart_ed").cast(DecimalType(10, 0))) \
    .withColumn("acuity_level", col("acuity_level").cast(StringType())) \
    .withColumn("birth_date", to_timestamp("birth_date")) \
    .withColumn("death_date", to_timestamp("death_date")) \
    .withColumn("arrival_dttm", to_timestamp("arrival_dttm")) \
    .withColumn("bed_reqst_dttm", to_timestamp("bed_reqst_dttm")) \
    .withColumn("depart_from_ed_dttm", to_timestamp("depart_from_ed_dttm")) \
    .withColumn("admsn_dttm", to_timestamp("admsn_dttm")) \
    .withColumn("dschrg_dttm", to_timestamp("dschrg_dttm")) \
    .withColumn("row_insert_tsp", lit(get_row_time())) \
    .withColumn("file_nm", lit(file_name)) \
    .withColumn("insert_user_id",lit(current_user)) \
    .withColumn("run_tsp", to_timestamp(lit(file_mod_time))) \
    .withColumn("run_id", lit(unix_timestamp())) \
    .write.format("delta").mode("append").saveAsTable("epic_rltm.rwb_currnt_admt_ptnt")

# COMMAND ----------

import itertools
from dateutil import tz

def prepare_data_to_post(data,batch_size=50):
  
  volumes,volumes_loc = [],[]
  bodies = []
  cst_timezone = tz.gettz('America/Chicago')
  
  all_data = data.collect()
  all_locations = list(set([row["location"] for row in all_data]))
  
  for location in all_locations:
    census_count_loc_rows = [row for row in all_data if row["location"] == location]
    for row in census_count_loc_rows:
      output_date = row["startTime"].astimezone(cst_timezone).date().strftime('%Y-%m-%d')
      output = {
                "date": output_date,
                "details": [
                  {
                    "planType": row["planType"],
                    "scheduleZone": {
                      "name": row["name"]
                    },
                    "value":  row["CENSUS_cnt"]
                  }
                      ],
                      "location": {
                        "qualifier": row["location"]
                }
               }
      volumes.append(output)
      
    volumes_loc.append(volumes)
    volumes = []
  
  access_token = get_access_token()

  volume_loc_batchs = [list(itertools.chain.from_iterable(batch)) for batch in generate_batchs_all(volumes_loc,batch_size)]
  
  for volume_loc_batch in volume_loc_batchs:
      body = {
        "volume" : {
          "do" : {
            "volumes" : volume_loc_batch
          }
        }
      }
      bodies.append(body)

  return bodies

# COMMAND ----------

def get_census_count():
  query = """
      with dep as (
      select distinct epic_dept_id, epic_dept_nm, cst_cntr_acctng_unit, dept_bus_strctr
      from ukg.ukg_dept_bus_strctr
      where epic_dept_id is not null and DEPT_BUS_STRCTR is not null and upper(frcst_yn) = 'YES' and run_id = (select max(run_id) from ukg.ukg_dept_bus_strctr)
      )
      , ptnt_Dtl as (
        select distinct ptnt.pat_enc_csn_id ,dep.epic_dept_id ,dep.dept_bus_strctr,ptnt.run_tsp, ptnt.run_id, ptnt.file_nm
        from epic_rltm.rwb_currnt_admt_ptnt ptnt
        inner join dep on cast(ptnt.encntr_dept_id as int) = cast(dep.epic_dept_id as int)
        where run_tsp > (select max(census_dttm) from ukg.ukg_open_census_post_data)  
      )
      , ltst_file as (
      select max(cast(substr(file_nm,9,12) as long)) as max_tsp from ptnt_Dtl
      )  --added criteria on 01/20/2023
    select dept_bus_strctr as location, run_tsp as census_dttm ,'actual' as Actual, count(pat_enc_csn_id) as census_cnt
    from ptnt_Dtl
    inner join ltst_file on cast(substr(ptnt_Dtl.file_nm,9,12) as long) = ltst_file.max_tsp
    group by dept_bus_strctr, run_tsp;
    """
  return spark.sql(query)

# COMMAND ----------

import os
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

import pytz
from azure.storage.filedatalake import DataLakeServiceClient

def get_adls_file_timestamp(file_name,container_name="idp-datalake",dir_path="UKG/Binary/RWB/Census"):
  cst_timezone = pytz.timezone("US/Central")
  service_client = DataLakeServiceClient(account_url = "https://{}.dfs.core.windows.net".format(storage_account_name),credential=access_key)
  container_client = service_client.get_file_system_client(file_system=container_name)
  directory_client = container_client.get_directory_client(dir_path)
  file_client = directory_client.get_file_client(file_name)
  file_properties = file_client.get_file_properties()
  file_latest_time_cst = file_properties["last_modified"].astimezone(cst_timezone)
  return file_latest_time_cst
  

# COMMAND ----------

import pytz
from azure.storage.filedatalake import DataLakeServiceClient

def get_adls_latest_dir_timestamp_recurse(container_name="idp-datalake",dir_path="UKG/Binary/RWB/Census"):
  cst_timezone = pytz.timezone("US/Central")
  service_client = DataLakeServiceClient(account_url = "https://{}.dfs.core.windows.net".format(storage_account_name),credential=access_key)
  container_client = service_client.get_file_system_client(file_system=container_name)
  directory_client = container_client.get_directory_client(dir_path)
  directory_properties = directory_client.get_directory_properties()
  directory_latest_time_cst = directory_properties["last_modified"].astimezone(cst_timezone)
  return directory_latest_time_cst
  

# COMMAND ----------

def get_last_modified_file_tm_recursive(container_name="idp-datalake",dir_path="UKG/Binary/RWB/Census"):
  all_files_latest_tms = []
  cst_timezone = pytz.timezone("US/Central")
  service_client = DataLakeServiceClient(account_url = "https://{}.dfs.core.windows.net".format(storage_account_name),credential=access_key)
  container_client = service_client.get_file_system_client(file_system=container_name)
  directory_client = container_client.get_directory_client(dir_path)
  all_paths = container_client.get_paths(path=dir_path)
  file_paths = [path.name for path in all_paths]
  for file_path in file_paths:
    relative_file_path = file_path.replace(dir_path,"")
    file_client = directory_client.get_file_client(relative_file_path)
    file_name = file_path[file_path.rfind("/")+1:]
    file_properties = file_client.get_file_properties()
    file_latest_time_cst = file_properties["last_modified"].astimezone(cst_timezone)
    all_files_latest_tms.append((file_name,file_latest_time_cst))
  return all_files_latest_tms
  

# COMMAND ----------

def get_staffing_descriptors(qualifiers_batchs,headers):
  staffing_descriptors =[]
  error_batch=[]
  stf_matrx_error=[]
  for batch in qualifiers_batchs:
    try:
        url = stfng_matrx_url
        payload = json.dumps(
            {
             "where": {
               "locations": {
                  "qualifiers": batch
                 }
             }
           }
           )
        response = json.loads(requests.post(url,headers = headers,data=payload).text)
  
        staffing_descriptors += [item["name"] for item in response]
    except Exception as error:
      print("Exception on batch messages : {}".format(error))
      error_batch += batch
      stf_matrx_error =[]
      err_batch = generate_err_batchs(error_batch,1)
      for x in err_batch:
        try:
            url = stfng_matrx_url
            payload = json.dumps(
              {
               "where": {
                 "locations": {
                    "qualifiers": x
                   }
               }
             }
             )
            response = json.loads(requests.post(url,headers = headers,data=payload).text)

            staffing_descriptors += [item["name"] for item in response]
        except Exception as error:
          print("Exception on single qualifier : {} - Exception : {}".format(x,error))
          stf_matrx_error += x  
        
    
  return staffing_descriptors,stf_matrx_error
  

# COMMAND ----------

# def get_staffing_descriptors(qualifiers,headers):
#   url = stfng_matrx_url
#   payload = json.dumps(
#     {
#       "where": {
#           "locations": {
#               "qualifiers": qualifiers
#           }
#       }
      
#   }
#   )
#   response = json.loads(requests.post(url,headers = headers,data=payload).text)
  
#   try:
#     staffing_descriptors = [item["name"] for item in response]
#   except Exception as error:
#     print(response)
#     raise Exception(error)
    
#   return staffing_descriptors
  

# COMMAND ----------

def apply_update_on_qualifier(qualifier_descriptor,headers,start_date,end_date):
  
  #end_date = datetime.now().date().strftime("%Y-%m-%d")
  payload = json.dumps({
    "generate": {
        "do": {
            "staffingMatrix": {
                "qualifier": qualifier_descriptor
            }
        },
        "where": {
            "actual": False,
            "budget": False,
            "plan": True,
            "dateRange": {
                "endDate": end_date.strftime("%Y-%m-%d"),
                "startDate": start_date.strftime("%Y-%m-%d")
            }
        }
    }
  })
  url = wrkload_gen_apply_update_url
  response = requests.post(url,headers=headers,data=payload)
  print("Qualifier Descriptor : {}".format(qualifier_descriptor))
  print("Status Code : {}".format(response.status_code))
  print("Response Text : {}".format(response.text))
  return [qualifier_descriptor,response.status_code,response.text]

# COMMAND ----------

def get_valid_qualifiers(table_name):
  df = spark.sql("SELECT * FROM {}".format(table_name))
  df_valid_qualifiers = df.where(col("mwod_yes_no")=="yes")\
                          .where(col("stf_matrx_yes_no")=='YES')\
                          .where(col("RUN_ID")==max_run_id)\
                          .select(col("dept_bus_strctr"))
  valid_qualifiers = [qualifiers["dept_bus_strctr"] for qualifiers in df_valid_qualifiers.collect()]
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

import numpy as np
import math
import decimal
import builtins as bi

def generate_batchs_all(batch_list,batch_size):
  output_batch = []
  num_splits = math.ceil(decimal.Decimal(len(batch_list))/decimal.Decimal(batch_size))
  indices_arr = np.array(list(range(len(batch_list))))
  indices_arr_batchs = np.array_split(indices_arr,num_splits)
  indices_batchs = [list(batch) for batch in indices_arr_batchs]
  for indices_batch in indices_batchs:
    output_batch.append(batch_list[bi.min(indices_batch):bi.max(indices_batch)+1])
  return output_batch

# COMMAND ----------

import numpy as np
import math
import decimal

def generate_err_batchs(error_qlfr,error_batch_size):
  num_splits = math.ceil(decimal.Decimal(len(error_qlfr))/decimal.Decimal(error_batch_size))
  qualifiers_arr = np.array(error_qlfr)
  qualifiers_arr_batchs = np.array_split(qualifiers_arr,num_splits)
  qualifiers_batchs = [list(batch) for batch in qualifiers_arr_batchs]
  return qualifiers_batchs

# COMMAND ----------

import smtplib 

# Here are the email package modules we'll need
from email.mime.image import MIMEImage
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText

def send_email(from_email, to_email, message):
  
  
  msg = MIMEMultipart()
  msg['Subject'] = '{}: Issue with Staffing Matrix in UKG'.format(environment)

  recipients = [to_email]
  msg['From'] = from_email
  msg['To'] = ', '.join(recipients)
  title = 'UKG Staffing Matrix'
  msg_content = '''<html>
    <head></head>
    <body>
      <h1>{0}</h1>
      <p>{1}</p>
    </body>
  </html>'''.format(title, message)
  msg.attach(MIMEText(msg_content, 'html'))
 

  s = smtplib.SMTP('smtp.mercy.net', 25)
  s.send_message(msg)
  s.quit()


# COMMAND ----------

import smtplib 

from email.mime.image import MIMEImage
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText

def send_expiration_notifications(expired_qualifiers,rolling_window):
  
  
  from_email = 'DoNotReply@Mercy.Net'
  to_email = 'edao_dsgm_realtime@mercy.net'
  
  msg = MIMEMultipart()
  msg['Subject'] = '{} Environment - Problems with Expiration Dates for Certain Locations / Qualifiers'.format(environment)

  recipients = [to_email]
  msg['From'] = from_email
  msg['To'] = ', '.join(recipients)
  
  title = 'Expired Location'
  message = "Locations  : {} have expiration date less than {} from today".format(",".join(expired_qualifiers),rolling_window)
  
  msg_content = '''<html>
    <head></head>
    <body>
      <h1>{0}</h1>
      <p>{1}</p>
    </body>
  </html>'''.format(title, message)
  msg.attach(MIMEText(msg_content, 'html'))
 

  s = smtplib.SMTP('smtp.mercy.net', 25)
  s.send_message(msg)
  s.quit()


# COMMAND ----------

def email_notify(msg):
  size1 = len(msg)

  from_email = 'DoNotReply@Mercy.Net'
  to_email = '{}'.format(ukg_email_notify)

  flag = False
  message = 'The hourly census load is not processing for the below mentioned business structure. Please address the issue.' + '<td><td><br>' + '<td><td><br>' + 'Following units are missing the staffing matrix in UKG.  '
  if size1 > 0:
    message = message + '<td><td><br>' + msg
    flag = True
  if flag == True:
    send_email(from_email, to_email, message)
  else:
    print("No Staffing matrix Error")

# COMMAND ----------

census_table_name = "epic_rltm.rwb_currnt_admt_ptnt"
mapping_table_name = "ukg.ukg_dept_bus_strctr"
census_data_dir_path = "{}/{}".format(os.getenv("adls_file_path"),"UKG/Binary/RWB/Census")
# mapping_data_dir_path = "{}/{}".format(os.getenv("adls_file_path"),"UKG/Mapping")
# mapping_data_file_name = "Mercy_Epic_to_UKG_Forcaster_Mapping_Table.csv"
update_qualifiers_logs_table_name = "ukg_updt_qlfr_post_log"
batch_size=50
error_batch_size=1

# COMMAND ----------

def post_open_census_data(data,rolling_window,files_processed):
    c_locations=data.select("location").union(data.withColumn("location",get_cost_center_udf(col("location"))).select("location"))\
               .union(data.withColumn("location",get_cost_center_5_udf(col("location"))).select("location")).distinct()
    #   skpathi - added above line code t0 pull in costcenters into locations as some business structures don't have scheduling zone data
    locations = [row["location"] for row in c_locations.select("location").distinct().collect()]
    effec_date = [row["census_dttm"] for row in data.select("census_dttm").distinct().collect()][0]
    #   Collect in above statement is converting date into UTC, below 3 lines of code is converting it to CST
    from_zone = tz.gettz('UTC')
    effec_date = effec_date.replace(tzinfo=from_zone)
    effec_date_cst = effec_date.astimezone(tz.gettz('America/Chicago')).date().strftime('%Y-%m-%d')

    access_token = get_access_token()

    expired_qualifiers , scheduled_zones = get_schedule_zones(locations, effec_date_cst, rolling_window)
    print("Retrieved Scheduled Zones....")

    if expired_qualifiers:
      send_expiration_notifications(expired_qualifiers,rolling_window)

    joined_zones_1 = data.join(scheduled_zones, ["location"], "inner") \
        .select(data.location, "CENSUS_DTTM", "CENSUS_cnt", "name", "startTime", "endTime", "effectiveDate") \
        .withColumn("effectiveDate", to_timestamp("effectiveDate")) \
        .withColumn("census_dttm", col("CENSUS_DTTM"))


    joined_zones_2 = data.join(scheduled_zones, get_cost_center_udf(data.location) == scheduled_zones.location, "inner") \
        .select(data.location, "CENSUS_DTTM", "CENSUS_cnt", "name", "startTime", "endTime", "effectiveDate") \
        .withColumn("effectiveDate", to_timestamp("effectiveDate")) \
        .withColumn("census_dttm", col("CENSUS_DTTM"))

    joined_zones_3 = data.join(scheduled_zones, get_cost_center_5_udf(data.location) == scheduled_zones.location, "inner") \
        .select(data.location, "CENSUS_DTTM", "CENSUS_cnt", "name", "startTime", "endTime", "effectiveDate") \
        .withColumn("effectiveDate", to_timestamp("effectiveDate")) \
        .withColumn("census_dttm", col("CENSUS_DTTM"))


    joined_zones = joined_zones_1.union(joined_zones_2).union(joined_zones_3).distinct()

    joined_zones = joined_zones.withColumn("planType",when((col("CENSUS_DTTM") >= col("startTime")) & (col("CENSUS_DTTM") <= col("endTime")),"ACTUAL").otherwise(lit("PLAN")))\
                               .where(col("endTime") >= col("CENSUS_DTTM"))
    print("Combined Scheduled Zones with Latest Census Data.....")


    update_url = volume_apply_update_url

    update_batchs = prepare_data_to_post(joined_zones,batch_size=20)
    print("Update Batchs : {}".format(len(update_batchs)))

    for batch_num,update_batch in enumerate(update_batchs):
      print("Batch Num : {}".format(batch_num))
      update_batch = json.dumps(update_batch)

      update_headers = {
        'appkey': appkey,
        'Authorization': get_access_token(),
        'Content-Type': 'application/json'
      }

      response = requests.request("POST", update_url, headers=update_headers, data=update_batch)
      response_code = response.status_code
      print("Response Code",response_code)
      print("Posted JSON to post data to UKG for rolling window of : {} days...".format(rolling_window))

      if str(response_code).startswith("2"):
        files_processed = ",".join(files_processed)
        joined_zones.withColumn("file_prcsd",lit(files_processed))\
        .withColumn("dept_bus_strctr",col("location"))\
        .withColumn("schdl_zn_nm",col("name"))\
        .withColumn("schdl_zn_start_tm",col("startTime"))\
        .withColumn("schdl_zn_end_tm",col("endTime"))\
        .withColumn("eff_dt",col("effectiveDate"))\
        .select("dept_bus_strctr","census_dttm","census_cnt","schdl_zn_nm","schdl_zn_start_tm","schdl_zn_end_tm","eff_dt","file_prcsd","planType")\
        .write.format("delta").option("mergeSchema","true").mode("append").save('{}/raw/ukg/ukg_open_census_post_data'.format(os.getenv("adls_file_path")))


    valid_qualifiers = get_valid_qualifiers(mapping_table_name)
    qualifiers_batchs = generate_batchs(valid_qualifiers,batch_size)
    stfng_matrx_qlfr = get_staffing_descriptors(qualifiers_batchs,update_headers)
    qualifier_descriptors = stfng_matrx_qlfr[0]
    stfng_matrx_error = stfng_matrx_qlfr[1]
    update_result_rows = []

    start_date = to_date(effec_date_cst)
    end_date = add_days(start_date,rolling_window)
    
    
    for qualifier in qualifier_descriptors:
      rows = apply_update_on_qualifier(qualifier,update_headers,start_date,end_date)
      update_result_rows.append(rows)

    print("Updated Qualifiers after posting to UKG.....")

    if len(update_result_rows) > 0:
      df = pd.DataFrame(update_result_rows,columns=["QualifierDescriptor","StatusCode","ResponseText"])
      update_logs_df = spark.createDataFrame(df)\
                          .withColumn("row_insert_tsp",lit(datetime.now()))\
                          .withColumn("qlfr_descrptr",col("QualifierDescriptor"))\
                          .withColumn("stat_cd",col("StatusCode"))\
                          .withColumn("resp_txt",col("ResponseText"))\
                          .select("qlfr_descrptr","stat_cd","resp_txt","row_insert_tsp")

      update_logs_df.write\
                  .format("delta")\
                  .mode("append")\
                  .saveAsTable(update_qualifiers_logs_table_name)

    if len(stfng_matrx_error) > 0:
        msg=' '
        for y in stfng_matrx_error:
          msg = msg +'<td><td><br>' +y+ '</td></td>'
        if len(msg) >1:
            email_notify(msg)


#     if len(stfng_matrx_error) > 0 and len(msg) > 1:      
#        email_notify(msg)   



# COMMAND ----------

rolling_window = 7
latest_rwb_file = dbutils.widgets.get("latest_open_census_file_name")

if latest_rwb_file:
  print("Processing File : {}".format(latest_rwb_file))
  preprocess_binary_data(latest_rwb_file)
  write_census_data("{}/{}".format(census_data_dir_path,latest_rwb_file))
  
census_count = get_census_count()

if census_count.count() > 0:
  dates = [row["census_dttm"] for row in census_count.select("census_dttm").distinct().collect()]
  for date in dates:
    files_processed = [latest_rwb_file if latest_rwb_file else ""]   
    census_partition = census_count.where(col("census_dttm") == date)
    post_open_census_data(census_partition,rolling_window,files_processed)
    