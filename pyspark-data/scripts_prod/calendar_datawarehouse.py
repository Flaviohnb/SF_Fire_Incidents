#!pip install psycopg2-binary

import sys
import subprocess
import pkg_resources

required = {'psycopg2-binary', 'pyspark'}
installed = {pkg.key for pkg in pkg_resources.working_set}
missing = required - installed

if missing:
    python = sys.executable
    subprocess.check_call([python, '-m', 'pip', 'install', *missing], stdout=subprocess.DEVNULL)

import pyspark.sql.functions as F
import datetime
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import psycopg2 
from pyspark.sql import SparkSession
from pyspark.sql.types import DateType
from datetime import datetime, timedelta

# Spark session & context
spark = (
    SparkSession
    .builder
    .config("spark.jars.packages", "org.postgresql:postgresql:42.5.2")
    .master("local")
    .getOrCreate()
)

sc = spark.sparkContext

# First date of dataframe
base_date_str = '2000/01/01'
base_date = datetime.strptime(base_date_str, '%Y/%m/%d')

# List of 100 years, from year 2000.
date_list = [base_date + timedelta(days=x) for x in range(36500)]

# Creating data frame from list
df_dimcalendario = spark.createDataFrame(date_list, DateType())

# Creating columns
df_dimcalendario_2 = (
    df_dimcalendario
    .select(
        F.to_date(F.col('value'), 'YYYY-mm-dd').alias('date_full')
        ,F.date_format(F.col('value'), "y").cast('int').alias('year')
        ,F.date_format(F.col('value'), "M").cast('int').alias('month')
        ,F.date_format(F.col('value'), "MMMM").alias('month_name')
        ,F.date_format(F.col('value'), "d").cast('int').alias('day')
        ,F.dayofweek(F.col('value')).cast('int').alias("day_of_week_number")
        ,F.date_format(F.col('value'), "EEEE").alias('day_of_week_name')
        ,F.dayofyear(F.col('value')).cast('int').alias("day_of_year")
        ,F.date_format(F.col('value'), "QQQ").alias('quarter_of_year')
        ,F.weekofyear(F.col('value')).cast('int').alias("week_of_year")
    )    
)


# Connection Params
properties = {
    "user": "postgres",
    "password": "Postgres123!",
    "driver": "org.postgresql.Driver"
}
url = "jdbc:postgresql://postgres-datawarehouse:5432/postgres"
result_table_name = "dim_calendar"

# Insert Spark Dataframe into public.dim_calendar
df_dimcalendario_2.write.jdbc(url, result_table_name, mode="overwrite", properties=properties)