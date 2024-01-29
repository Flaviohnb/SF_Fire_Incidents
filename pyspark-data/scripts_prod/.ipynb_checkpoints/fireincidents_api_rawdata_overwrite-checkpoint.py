#!pip install sodapy

import sys
import subprocess
import pkg_resources

required = {'sodapy'}
installed = {pkg.key for pkg in pkg_resources.working_set}
missing = required - installed

if missing:
    python = sys.executable
    subprocess.check_call([python, '-m', 'pip', 'install', *missing], stdout=subprocess.DEVNULL)

from sodapy import Socrata
from pyspark.sql import SparkSession
import datetime
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq

# Spark session & context
spark = SparkSession.builder.master("local").getOrCreate()
sc = spark.sparkContext

# client Socrata & "response"
client = Socrata("data.sfgov.org", None)
results = client.get_all("wr8u-xric") 

# Convert to pandas DataFrame
pandasDF = pd.DataFrame.from_records(results)

# Convert DataFrame to Apache Arrow Table
table = pa.Table.from_pandas(pandasDF)

# Save dataframe on datalake 
pq.write_table(table, "/home/jovyan/datalake/raw-data/fire_incidents.parquet", compression='GZIP')