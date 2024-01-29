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

from pyspark.sql import SparkSession
import psycopg2

# Spark session & context
spark = (
    SparkSession
    .builder
    .config("spark.jars.packages", "org.postgresql:postgresql:42.5.2")
    .master("local")
    .getOrCreate()
)

sc = spark.sparkContext

# Read Consumer Data from data lake
consumerData_fireIncidents = spark.read.parquet('/home/jovyan/datalake/consumer-data/fire_incidents/*.parquet')

# Connection Params
properties = {
    "user": "postgres",
    "password": "Postgres123!",
    "driver": "org.postgresql.Driver"
}
url = "jdbc:postgresql://postgres-datawarehouse:5432/postgres"
result_table_name = "fire_incidents"

# Insert Spark Dataframe into public.fire_incidents
consumerData_fireIncidents.write.jdbc(url, result_table_name, mode="overwrite", properties=properties)