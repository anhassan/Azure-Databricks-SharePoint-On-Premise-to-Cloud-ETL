# Databricks notebook source
# TYPE: PYTHON Commands
# DEFINITION: This notebook loads OpenCensus Reporting Workbench files and posts data to API
# 
# CHANGE HISTORY
# ---------------------------------------------------------------------------------------------------
# DATE           DEVELOPER         DESCRIPTION
# 01/01/2023     AHASSAN2          Initial creation


# COMMAND ----------

# MAGIC %run /Shared/utils/env_var

# COMMAND ----------

# MAGIC %run ./ukg_open_census_file_cleanser

# COMMAND ----------

from pyspark.sql.functions import *
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

def get_schedule_zones(access_token, locations, effective_date, expiry_date,include_cost_center=True):
  prev_date_cst = (datetime.strptime(effective_date, '%Y-%m-%d') + timedelta(days=-1)).strftime('%Y-%m-%d')
  next_date_cst = (datetime.strptime(effective_date, '%Y-%m-%d') + timedelta(days=1)).strftime('%Y-%m-%d')
  zones_url = schdl_zones_url
  zones_payload = json.dumps({
    "where": {
      "effectiveDate": effective_date,
      "expirationDate": expiry_date,
      "locations": {
        "qualifiers": locations
        }
      }
    })
  
  zones_headers = {
    'appkey': appkey,
    'Authorization': access_token,
    'Content-Type': 'application/json'
  }

  response = requests.request("POST", zones_url, headers=zones_headers, data=zones_payload)

  df = spark.read.json(sc.parallelize([response.text]))

  bs_zones_curr_dt = df.select("effectiveDate", "expirationDate", "location", explode("scheduleZoneSet.scheduleZones").alias("zones")) \
    .withColumn("location", col("location").qualifier) \
    .withColumn("description", col("zones.description")) \
    .withColumn("startTime", to_timestamp(col("zones.startTime"))) \
    .withColumn("endTime", to_timestamp(col("zones.endTime"))) \
    .withColumn("name", col("zones.name")) \
    .withColumn("cost_center", get_cost_center_udf(col("location"))) \
    .withColumn("startTime", from_unixtime(unix_timestamp("startTime", "HH:MM:SS"), "{} HH:00:00".format(effective_date))) \
    .withColumn("endTime", from_unixtime(unix_timestamp("endTime", "HH:MM:SS"), "{} HH:00:00".format(effective_date))) \
    .withColumn("endTime", when(col("endTime") < col("startTime"),(col("endTime") + expr("interval 1 days"))).otherwise(col("endTime")))
  
  
  zones_payload = json.dumps({
    "where": {
      "effectiveDate": prev_date_cst,
      "expirationDate": prev_date_cst,
      "locations": {
        "qualifiers": locations
        }
      }
    })
  
  zones_headers = {
    'appkey': appkey,
    'Authorization': access_token,
    'Content-Type': 'application/json'
  }

  response = requests.request("POST", zones_url, headers=zones_headers, data=zones_payload)

  df = spark.read.json(sc.parallelize([response.text]))

  bs_zones_prev_dt = df.select("effectiveDate", "expirationDate", "location", explode("scheduleZoneSet.scheduleZones").alias("zones")) \
    .withColumn("location", col("location").qualifier) \
    .withColumn("description", col("zones.description")) \
    .withColumn("startTime", to_timestamp(col("zones.startTime"))) \
    .withColumn("endTime", to_timestamp(col("zones.endTime"))) \
    .withColumn("name", col("zones.name")) \
    .withColumn("cost_center", get_cost_center_udf(col("location"))) \
    .withColumn("startTime", from_unixtime(unix_timestamp("startTime", "HH:MM:SS"), "{} HH:00:00".format(prev_date_cst))) \
    .withColumn("endTime", from_unixtime(unix_timestamp("endTime", "HH:MM:SS"), "{} HH:00:00".format(prev_date_cst))) \
    .withColumn("endTime", when(col("endTime") < col("startTime"),(col("endTime") + expr("interval 1 days"))).otherwise(col("endTime"))) \
    .withColumn("row_number", row_number().over(Window.partitionBy("location").orderBy(col("startTime").desc()))) \
    .filter(col("row_number") == 1) \
    .drop(col("row_number"))
  
  
  zones_payload = json.dumps({
    "where": {
      "effectiveDate": next_date_cst,
      "expirationDate": next_date_cst,
      "locations": {
        "qualifiers": locations
        }
      }
    })
  
  zones_headers = {
    'appkey': appkey,
    'Authorization': access_token,
    'Content-Type': 'application/json'
  }

  response = requests.request("POST", zones_url, headers=zones_headers, data=zones_payload)

  df = spark.read.json(sc.parallelize([response.text]))

  bs_zones_next_dt = df.select("effectiveDate", "expirationDate", "location", explode("scheduleZoneSet.scheduleZones").alias("zones")) \
    .withColumn("location", col("location").qualifier) \
    .withColumn("description", col("zones.description")) \
    .withColumn("startTime", to_timestamp(col("zones.startTime"))) \
    .withColumn("endTime", to_timestamp(col("zones.endTime"))) \
    .withColumn("name", col("zones.name")) \
    .withColumn("cost_center", get_cost_center_udf(col("location"))) \
    .withColumn("startTime", from_unixtime(unix_timestamp("startTime", "HH:MM:SS"), "{} HH:00:00".format(next_date_cst))) \
    .withColumn("endTime", from_unixtime(unix_timestamp("endTime", "HH:MM:SS"), "{} HH:00:00".format(next_date_cst))) \
    .withColumn("endTime", when(col("endTime") < col("startTime"),(col("endTime") + expr("interval 1 days"))).otherwise(col("endTime"))) \
    .withColumn("row_number", row_number().over(Window.partitionBy("location").orderBy(col("startTime").asc()))) \
    .filter(col("row_number") == 1) \
    .drop(col("row_number"))
  
  bs_zones = bs_zones_curr_dt.union(bs_zones_prev_dt).union(bs_zones_next_dt)  


  empty_locations = [row["cost_center"] for row in bs_zones.filter(col("zones").isNull()).select("cost_center").distinct().collect()]
  
  if len(empty_locations) > 0 and include_cost_center:
    zones_payload = json.dumps({
      "where": {
        "effectiveDate": effective_date,
        "expirationDate": expiry_date,
        "locations": {
          "qualifiers": empty_locations
          }
        }
      })
    
    response = requests.request("POST", zones_url, headers=zones_headers, data=zones_payload)
    cost_zones_raw = spark.read.json(sc.parallelize([response.text]))
    parsed_cost_zones = cost_zones_raw.select("effectiveDate", "expirationDate", "location", explode("scheduleZoneSet.scheduleZones").alias("c_zones")) \
      .withColumn("c_location", col("location").qualifier) \
      .withColumn("c_description", col("zones.description")) \
      .withColumn("c_startTime", to_timestamp(col("zones.startTime"))) \
      .withColumn("c_endTime", to_timestamp(col("zones.endTime"))) \
      .withColumn("c_startTime", from_unixtime(unix_timestamp("c_startTime", "HH:MM:SS"), "{} HH:00:00".format(effective_date))) \
      .withColumn("c_endTime", from_unixtime(unix_timestamp("c_endTime", "HH:MM:SS"), "{} HH:00:00".format(effective_date))) \
      .withColumn("c_name", col("zones.name")) \
      .withColumn("c_cost_center", get_cost_center_udf(col("location"))) \
      .select("c_location", "c_description", "c_startTime", "c_endTime", "c_name", "c_cost_center")
    
    return bs_zones.join(parsed_cost_zones, bs_zones("cost_center") == parsed_cost_zones("c_cost_center"), "left") \
                    .select("location", coalesce(col("description"), col("c_description")).alias("description"), \
                                        coalesce(col("startTime"), col("c_startTime")).alias("startTime"), \
                                        coalesce(col("endTime"), col("c_endTime")).alias("endTime"), \
                                        coalesce(col("name"), col("c_name")).alias("name"),)
  else: 
    return bs_zones


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

def prepare_data_to_post(data):
  census_count_rows = data.collect()

  volumes = []
  
  access_token = get_access_token()

  for row in census_count_rows:

    actual_date = datetime.strptime(row["startTime"], '%Y-%m-%d %H:%M:%S').date().strftime('%Y-%m-%d')
    plan_date = datetime.strptime(row["next_zone_dttm"], '%Y-%m-%d %H:%M:%S').date().strftime('%Y-%m-%d')

    actual = {
      "date": actual_date,
      "details": [
        {
          "planType": "ACTUAL",
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

    plan = {
      "date": plan_date,
      "details": [
        {
          "planType": "PLAN",
          "scheduleZone": {
            "name": row["next_zone_name"]
          },
          "value":  row["CENSUS_cnt"]
        }
			],
			"location": {
			  "qualifier": row["location"]
      }
		}
    volumes.append(actual)
    volumes.append(plan)

  body = {
    "volume" : {
      "do" : {
        "volumes" : volumes
      }
    }
  }

  return body

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

!pip install azure-storage-file-datalake

# COMMAND ----------

import pytz
from azure.storage.filedatalake import DataLakeServiceClient

def get_adls_file_timestamp(file_name,container_name="idp-datalake",dir_path="UKG/RWB/Census"):
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

def get_adls_latest_dir_timestamp_recurse(container_name="idp-datalake",dir_path="UKG/RWB/Census"):
  cst_timezone = pytz.timezone("US/Central")
  service_client = DataLakeServiceClient(account_url = "https://{}.dfs.core.windows.net".format(storage_account_name),credential=access_key)
  container_client = service_client.get_file_system_client(file_system=container_name)
  directory_client = container_client.get_directory_client(dir_path)
  directory_properties = directory_client.get_directory_properties()
  directory_latest_time_cst = directory_properties["last_modified"].astimezone(cst_timezone)
  return directory_latest_time_cst
  

# COMMAND ----------

def get_last_modified_file_tm_recursive(container_name="idp-datalake",dir_path="UKG/RWB/Census"):
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
            response = json.loads(requests.post(url,headers = update_headers,data=payload).text)

            staffing_descriptors += [item["name"] for item in response]
        except Exception as error:
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

def apply_update_on_qualifier(qualifier_descriptor,headers):
  start_date = datetime.now().date().strftime("%Y-%m-%d")
  
  end_date = datetime.now().date().strftime("%Y-%m-%d")
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
                "endDate": end_date,
                "startDate": start_date
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
  msg['Subject'] = '{} Environment - Staffing Matrix'.format(environment)

  recipients = [to_email]
  msg['From'] = from_email
  msg['To'] = ', '.join(recipients)
  title = 'Staffing Matrix'
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
  to_email = 'edao_dsgm_realtime@mercy.net'
  flag = False
  message = 'Following Business Structures Need to be fixed for Staffing Matrix in UKG:  '
  if size1 > 0:
    #message = 'databricks : '
    message = message + msg
  #   print(message)
    flag = True
  if flag == True:
    send_email(from_email, to_email, message)
  else:
    print("No Staffing matrix Error")

# COMMAND ----------

census_table_name = "epic_rltm.rwb_currnt_admt_ptnt"
mapping_table_name = "ukg.ukg_dept_bus_strctr"
census_data_dir_path = "{}/{}".format(os.getenv("adls_file_path"),"UKG/RWB/Census")
# mapping_data_dir_path = "{}/{}".format(os.getenv("adls_file_path"),"UKG/Mapping")
# mapping_data_file_name = "Mercy_Epic_to_UKG_Forcaster_Mapping_Table.csv"
update_qualifiers_logs_table_name = "ukg_updt_qlfr_post_log"
batch_size=50
error_batch_size=1

# COMMAND ----------

def post_open_census_data(data):
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

  scheduled_zones = get_schedule_zones(get_access_token(), locations, effec_date_cst, effec_date_cst)
  
  joined_zones_1 = data.join(scheduled_zones, ["location"], "inner") \
      .select(data.location, "CENSUS_DTTM", "CENSUS_cnt", "name", "startTime", "endTime", "effectiveDate") \
      .filter((col("CENSUS_DTTM") >= col("startTime")) & (col("CENSUS_DTTM") <= col("endTime"))) \
      .withColumn("effectiveDate", to_timestamp("effectiveDate")) \
      .withColumn("census_dttm", col("CENSUS_DTTM"))
  
  
  joined_zones_2 = data.join(scheduled_zones, get_cost_center_udf(data.location) == scheduled_zones.location, "inner") \
      .select(data.location, "CENSUS_DTTM", "CENSUS_cnt", "name", "startTime", "endTime", "effectiveDate") \
      .filter((col("CENSUS_DTTM") >= col("startTime")) & (col("CENSUS_DTTM") <= col("endTime"))) \
      .withColumn("effectiveDate", to_timestamp("effectiveDate")) \
      .withColumn("census_dttm", col("CENSUS_DTTM"))
  
  joined_zones_3 = data.join(scheduled_zones, get_cost_center_5_udf(data.location) == scheduled_zones.location, "inner") \
      .select(data.location, "CENSUS_DTTM", "CENSUS_cnt", "name", "startTime", "endTime", "effectiveDate") \
      .filter((col("CENSUS_DTTM") >= col("startTime")) & (col("CENSUS_DTTM") <= col("endTime"))) \
      .withColumn("effectiveDate", to_timestamp("effectiveDate")) \
      .withColumn("census_dttm", col("CENSUS_DTTM"))
  
  
  joined_zones = joined_zones_1.union(joined_zones_2).union(joined_zones_3).distinct()
  
  added_next_zone_1 = joined_zones_1.join(scheduled_zones.withColumn("next_zone_dttm", col("startTime")) \
                                                       .withColumn("next_zone_name", col("name")) \
                                                       .select("location", "next_zone_dttm", "next_zone_name"), ["location"], "left") \
                                                       .filter(col("endTime") == col("next_zone_dttm"))

  added_next_zone_2 = joined_zones_2.join(scheduled_zones.withColumn("next_zone_dttm", col("startTime")) \
                                                    .withColumn("next_zone_name", col("name")) \
                                                    .select("location", "next_zone_dttm", "next_zone_name"),get_cost_center_udf(joined_zones_2.location) == scheduled_zones.location , "left") \
                                                    .filter(col("endTime") == col("next_zone_dttm")) \
                                                    .drop(scheduled_zones.location)



  added_next_zone_3 = joined_zones_3.join(scheduled_zones.withColumn("next_zone_dttm", col("startTime")) \
                                                   .withColumn("next_zone_name", col("name")) \
                                                   .select("location", "next_zone_dttm", "next_zone_name"),get_cost_center_5_udf(joined_zones_3.location) == scheduled_zones.location , "left") \
                                                   .filter(col("endTime") == col("next_zone_dttm"))\
                                                   .drop(scheduled_zones.location)

  added_next_zone = added_next_zone_1.union(added_next_zone_2).union(added_next_zone_3).distinct()


  update_url = volume_apply_update_url
  update_payload = json.dumps(prepare_data_to_post(added_next_zone))

  update_headers = {
    'appkey': appkey,
    'Authorization': get_access_token(),
    'Content-Type': 'application/json'
  }

  response = requests.request("POST", update_url, headers=update_headers, data=update_payload)
  response_code = response.status_code

  if str(response_code).startswith("2"):
    files_processed = ",".join(new_files)
    added_next_zone.withColumn("file_prcsd",lit(files_processed))\
    .withColumn("dept_bus_strctr",col("location"))\
    .withColumn("schdl_zn_nm",col("name"))\
    .withColumn("schdl_zn_start_tm",col("startTime"))\
    .withColumn("schdl_zn_end_tm",col("endTime"))\
    .withColumn("eff_dt",col("effectiveDate"))\
    .withColumn("next_schdl_zn_dttm",col("next_zone_dttm"))\
    .withColumn("next_schdl_zn_nm",col("next_zone_name"))\
    .select("dept_bus_strctr","census_dttm","census_cnt","schdl_zn_nm","schdl_zn_start_tm","schdl_zn_end_tm","eff_dt","next_schdl_zn_dttm","next_schdl_zn_nm","file_prcsd")\
    .write.format("delta").option("mergeSchema","true").mode("append").save('{}/raw/ukg/ukg_open_census_post_data'.format(os.getenv("adls_file_path")))
    
  
    valid_qualifiers = get_valid_qualifiers(mapping_table_name)
    qualifiers_batchs = generate_batchs(valid_qualifiers,batch_size)
    stfng_matrx_qlfr = get_staffing_descriptors(qualifiers_batchs,update_headers)
    qualifier_descriptors = stfng_matrx_qlfr[0]
    stfng_matrx_error = stfng_matrx_qlfr[1]
    update_result_rows = []
    
    for qualifier in qualifier_descriptors:
      rows = apply_update_on_qualifier(qualifier,update_headers)
      update_result_rows.append(rows)
  
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
                  .save('{}/raw/ukg/{}'.format(os.getenv("adls_file_path"),update_qualifiers_logs_table_name))
      
    if len(stfng_matrx_error) > 0:
        msg=' '
        for y in stfng_matrx_error:
          msg = msg +'<td><td><br>' +y+ '</td></td>'
        if len(msg) >1:
            email_notify(msg)
            
          
#     if len(stfng_matrx_error) > 0 and len(msg) > 1:      
#        email_notify(msg)   


# COMMAND ----------

# def post_open_census_data(data):
#   c_locations=data.select("location").union(data.withColumn("location",get_cost_center_udf(col("location"))).select("location"))\
#              .union(data.withColumn("location",get_cost_center_5_udf(col("location"))).select("location")).distinct()
# #   skpathi - added above line code t0 pull in costcenters into locations as some business structures don't have scheduling zone data
#   locations = [row["location"] for row in c_locations.select("location").distinct().collect()]
#   effec_date = [row["census_dttm"] for row in data.select("census_dttm").distinct().collect()][0]
# #   Collect in above statement is converting date into UTC, below 3 lines of code is converting it to CST
#   from_zone = tz.gettz('UTC')
#   effec_date = effec_date.replace(tzinfo=from_zone)
#   effec_date_cst = effec_date.astimezone(tz.gettz('America/Chicago')).date().strftime('%Y-%m-%d')
  
#   access_token = get_access_token()

#   scheduled_zones = get_schedule_zones(get_access_token(), locations, effec_date_cst, effec_date_cst)
  
#   joined_zones_1 = data.join(scheduled_zones, ["location"], "inner") \
#       .select(data.location, "CENSUS_DTTM", "CENSUS_cnt", "name", "startTime", "endTime", "effectiveDate") \
#       .filter((col("CENSUS_DTTM") >= col("startTime")) & (col("CENSUS_DTTM") <= col("endTime"))) \
#       .withColumn("effectiveDate", to_timestamp("effectiveDate")) \
#       .withColumn("census_dttm", col("CENSUS_DTTM"))
  
  
#   joined_zones_2 = data.join(scheduled_zones, get_cost_center_udf(data.location) == scheduled_zones.location, "inner") \
#       .select(data.location, "CENSUS_DTTM", "CENSUS_cnt", "name", "startTime", "endTime", "effectiveDate") \
#       .filter((col("CENSUS_DTTM") >= col("startTime")) & (col("CENSUS_DTTM") <= col("endTime"))) \
#       .withColumn("effectiveDate", to_timestamp("effectiveDate")) \
#       .withColumn("census_dttm", col("CENSUS_DTTM"))
  
#   joined_zones_3 = data.join(scheduled_zones, get_cost_center_5_udf(data.location) == scheduled_zones.location, "inner") \
#       .select(data.location, "CENSUS_DTTM", "CENSUS_cnt", "name", "startTime", "endTime", "effectiveDate") \
#       .filter((col("CENSUS_DTTM") >= col("startTime")) & (col("CENSUS_DTTM") <= col("endTime"))) \
#       .withColumn("effectiveDate", to_timestamp("effectiveDate")) \
#       .withColumn("census_dttm", col("CENSUS_DTTM"))
  
  
#   joined_zones = joined_zones_1.union(joined_zones_2).union(joined_zones_3).distinct()
  
#   added_next_zone_1 = joined_zones_1.join(scheduled_zones.withColumn("next_zone_dttm", col("startTime")) \
#                                                        .withColumn("next_zone_name", col("name")) \
#                                                        .select("location", "next_zone_dttm", "next_zone_name"), ["location"], "left") \
#                                                        .filter(col("endTime") == col("next_zone_dttm"))

#   added_next_zone_2 = joined_zones_2.join(scheduled_zones.withColumn("next_zone_dttm", col("startTime")) \
#                                                        .withColumn("next_zone_name", col("name")) \
#                                                        .select("location", "next_zone_dttm", "next_zone_name"),get_cost_center_udf(joined_zones_2.location) == scheduled_zones.location , "left") \
#                                                        .filter(col("endTime") == col("next_zone_dttm")) \
#                                                        .drop(scheduled_zones.location)



#   added_next_zone_3 = joined_zones_3.join(scheduled_zones.withColumn("next_zone_dttm", col("startTime")) \
#                                                        .withColumn("next_zone_name", col("name")) \
#                                                        .select("location", "next_zone_dttm", "next_zone_name"),get_cost_center_5_udf(joined_zones_3.location) == scheduled_zones.location , "left") \
#                                                        .filter(col("endTime") == col("next_zone_dttm"))\
#                                                        .drop(scheduled_zones.location)

#   added_next_zone = added_next_zone_1.union(added_next_zone_2).union(added_next_zone_3).distinct()


#   update_url = volume_apply_update_url
#   update_payload = json.dumps(prepare_data_to_post(added_next_zone))

#   update_headers = {
#     'appkey': appkey,
#     'Authorization': get_access_token(),
#     'Content-Type': 'application/json'
#   }

#   response = requests.request("POST", update_url, headers=update_headers, data=update_payload)
#   response_code = response.status_code

#   if str(response_code).startswith("2"):
#     files_processed = ",".join(new_files)
#     added_next_zone.withColumn("file_prcsd",lit(files_processed))\
#     .withColumn("dept_bus_strctr",col("location"))\
#     .withColumn("schdl_zn_nm",col("name"))\
#     .withColumn("schdl_zn_start_tm",col("startTime"))\
#     .withColumn("schdl_zn_end_tm",col("endTime"))\
#     .withColumn("eff_dt",col("effectiveDate"))\
#     .withColumn("next_schdl_zn_dttm",col("next_zone_dttm"))\
#     .withColumn("next_schdl_zn_nm",col("next_zone_name"))\
#     .select("dept_bus_strctr","census_dttm","census_cnt","schdl_zn_nm","schdl_zn_start_tm","schdl_zn_end_tm","eff_dt","next_schdl_zn_dttm","next_schdl_zn_nm","file_prcsd")\
#     .write.format("delta").option("mergeSchema","true").mode("append").save('{}/raw/ukg/ukg_open_census_post_data'.format(os.getenv("adls_file_path")))
    
#     valid_qualifiers = get_valid_qualifiers(mapping_table_name)
#     qualifier_descriptors = get_staffing_descriptors(valid_qualifiers,update_headers)
#     update_result_rows = []
    
#     for qualifier in qualifier_descriptors:
#       rows = apply_update_on_qualifier(qualifier,update_headers)
#       update_result_rows.append(rows)
  
#     if len(update_result_rows) > 0:
#       df = pd.DataFrame(update_result_rows,columns=["QualifierDescriptor","StatusCode","ResponseText"])
#       update_logs_df = spark.createDataFrame(df)\
#                           .withColumn("row_insert_tsp",lit(datetime.now()))\
#                           .withColumn("qlfr_descrptr",col("QualifierDescriptor"))\
#                           .withColumn("stat_cd",col("StatusCode"))\
#                           .withColumn("resp_txt",col("ResponseText"))\
#                           .select("qlfr_descrptr","stat_cd","resp_txt","row_insert_tsp")
      
#       update_logs_df.write\
#                   .format("delta")\
#                   .mode("append")\
#                   .save('{}/raw/ukg/{}'.format(os.getenv("adls_file_path"),update_qualifiers_logs_table_name))


# COMMAND ----------

new_files = get_unprocessed_files(census_data_dir_path,census_table_name)

for file in new_files:
  print("Processing File : {}".format(file))
  write_census_data("{}/{}".format(census_data_dir_path,file))
  
census_count = get_census_count()



if census_count.count() > 0:
  dates = [row["census_dttm"] for row in census_count.select("census_dttm").distinct().collect()]
  for date in dates:    
    census_partition = census_count.where(col("census_dttm") == date)

    post_open_census_data(census_partition) 
