# Databricks notebook source
# MAGIC %pip install pyodbc

# COMMAND ----------

from pyspark.sql.functions import *
from pyspark.sql.types import *
from datetime import datetime
from zoneinfo import ZoneInfo
from pyspark.sql.functions import col, sum as _sum, current_timestamp, max as _max

date = datetime.now(ZoneInfo("Europe/London")).date()
date_str = date.strftime("%Y-%m-%d")

# COMMAND ----------

# MAGIC %run Shared/Notebooks/ENV/env

# COMMAND ----------

dbutils.widgets.dropdown(name='Process_type', defaultValue='daily_sales', choices=['daily_sales','inbound_deliveries'], label='Select Process Type')
process_type = dbutils.widgets.get('Process_type')

# COMMAND ----------

dbutils.widgets.text(name='date',defaultValue=date_str,label='date')
target_date = dbutils.widgets.get('date')

# COMMAND ----------

jdbc_url = dbutils.secrets.get(scope='rsclp-scope', key='jdbc-url')
database = dbutils.secrets.get(scope='rsclp-scope', key='rsclp-database')
username = dbutils.secrets.get(scope='rsclp-scope', key='rsclp-db-username')
password = dbutils.secrets.get(scope='rsclp-scope', key='rsclp-db-password')
driver = dbutils.secrets.get(scope='rsclp-scope', key='rsclp-db-driver')
hostname = dbutils.secrets.get(scope='rsclp-scope', key='rsclp-db-hostname')
port = dbutils.secrets.get(scope='rsclp-scope', key='rsclp-db-port')

# COMMAND ----------

# 1. Get watermark
watermark_df = spark.read.format('jdbc') \
    .option('url', jdbc_url) \
    .option('dbtable', 'dbo.InventoryProcessWatermark') \
    .option('user', username) \
    .option('password', password) \
    .load()

# COMMAND ----------

if process_type == 'daily_sales':
    watermark_ts = watermark_df.filter(col('ProcessType') == 'Daily Sales') \
        .select(col('LastUpdatedOn')).collect()[0]["LastUpdatedOn"]
    df = spark.sql(f"""
        SELECT ProductID, StoreID, TotalQuantity, LoadTimestamp
        FROM {ENV_CATALOG}.rsmas_gold_schema.daily_sales 
        WHERE TransactionDate = '{target_date}' 
          AND LoadTimestamp > TIMESTAMP '{watermark_ts}'
    """).select(
        col('ProductID'), col('StoreID'), 
        (col('TotalQuantity') * -1).alias('DeltaQuantity'),
       from_utc_timestamp(current_timestamp(), "Europe/London").alias("UpdateTimestamp")
    )


elif process_type == 'inbound_deliveries':
    watermark_ts = watermark_df.filter(col('ProcessType') == 'Inbound Deliveries') \
        .select(col('LastUpdatedOn')).collect()[0]["LastUpdatedOn"]
    
    df = spark.sql(f"""
        SELECT ProductID, StoreID, DeliveredQuantity as TotalQuantity, LoadTimestamp
        FROM {ENV_CATALOG}.rsmas_gold_schema.inbound_deliveries 
        WHERE load_date = '{target_date}' 
          AND LoadTimestamp > TIMESTAMP '{watermark_ts}'
    """).select(
        col('ProductID'), col('StoreID'), 
        col('TotalQuantity').alias('DeltaQuantity'),
        from_utc_timestamp(current_timestamp(), "Europe/London").alias("UpdateTimestamp")
    )



# COMMAND ----------

df.write \
    .format("jdbc") \
    .mode("append") \
    .option("url", jdbc_url) \
    .option("dbtable", "ProductInventory_Staging") \
    .option("user", username) \
    .option("password", password) \
    .option("driver", "com.microsoft.sqlserver.jdbc.SQLServerDriver") \
    .save()

