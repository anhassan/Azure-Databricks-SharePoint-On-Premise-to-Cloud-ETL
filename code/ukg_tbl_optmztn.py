# Databricks notebook source
import os
import delta
import pyspark.sql.functions as F
from datetime import date, timedelta


# COMMAND ----------

retention_prd = int(dbutils.widgets.get("retention_prd"))

# COMMAND ----------

# MAGIC %sql
# MAGIC OPTIMIZE ukg.ukg_wrkload_dtl

# COMMAND ----------

# MAGIC %sql
# MAGIC OPTIMIZE ukg.ukg_wrkload_dtl_attr

# COMMAND ----------

# MAGIC %sql
# MAGIC OPTIMIZE ukg.ukg_stf_matrx 

# COMMAND ----------

# MAGIC %sql
# MAGIC OPTIMIZE ukg.ukg_stf_matrx_range 

# COMMAND ----------

# MAGIC %sql
# MAGIC OPTIMIZE ukg.ukg_dept_bus_strctr 

# COMMAND ----------

dt = delta.DeltaTable.forName(spark, 'ukg.ukg_wrkload_dtl')
dt.delete(F.col("row_insert_tsp") < (date.today() - timedelta(days=retention_prd)))

# COMMAND ----------

dt = delta.DeltaTable.forName(spark, 'ukg.ukg_wrkload_dtl_attr')
dt.delete(F.col("row_insert_tsp") < (date.today() - timedelta(days=retention_prd)))

# COMMAND ----------

dt = delta.DeltaTable.forName(spark, 'ukg.ukg_stf_matrx')
dt.delete(F.col("row_insert_tsp") < (date.today() - timedelta(days=retention_prd)))

# COMMAND ----------

dt = delta.DeltaTable.forName(spark, 'ukg.ukg_stf_matrx_range')
dt.delete(F.col("row_insert_tsp") < (date.today() - timedelta(days=retention_prd)))

# COMMAND ----------

dt = delta.DeltaTable.forName(spark, 'ukg.ukg_dept_bus_strctr')
dt.delete(F.col("row_insert_tsp") < (date.today() - timedelta(days=retention_prd)))

# COMMAND ----------

# MAGIC %sql
# MAGIC VACUUM ukg.ukg_wrkload_dtl

# COMMAND ----------

# MAGIC %sql
# MAGIC VACUUM ukg.ukg_wrkload_dtl_attr

# COMMAND ----------

# MAGIC %sql
# MAGIC VACUUM ukg.ukg_stf_matrx

# COMMAND ----------

# MAGIC %sql
# MAGIC VACUUM ukg.ukg_stf_matrx_range

# COMMAND ----------

# MAGIC %sql
# MAGIC VACUUM ukg.ukg_dept_bus_strctr