#!pip install sodapy

import sys
import subprocess
import pkg_resources

required = {'sodapy', 'pyspark'}
installed = {pkg.key for pkg in pkg_resources.working_set}
missing = required - installed

if missing:
    python = sys.executable
    subprocess.check_call([python, '-m', 'pip', 'install', *missing], stdout=subprocess.DEVNULL)

from sodapy import Socrata
from pyspark.sql import SparkSession
import pyspark.sql.functions as F
import datetime
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq

# Spark session & context
spark = SparkSession.builder.master("local").getOrCreate()
sc = spark.sparkContext

# Read Raw Data from data lake
rawData_fireIncidents = spark.read.parquet('/home/jovyan/datalake/raw-data/fire_incidents.parquet')

# Some transformations... (datatype)
consumerData_fireIncidents = (
    rawData_fireIncidents
    .select(
        rawData_fireIncidents.incident_number,
        rawData_fireIncidents.exposure_number.cast('int'),
        rawData_fireIncidents.id,
        rawData_fireIncidents.address,
        F.to_timestamp(rawData_fireIncidents.incident_date, "yyyy-MM-dd'T'HH:mm:ss").alias('incident_date'),
        rawData_fireIncidents.call_number,
        F.to_timestamp(rawData_fireIncidents.alarm_dttm, "yyyy-MM-dd'T'HH:mm:ss").alias('alarm_dttm'),
        F.to_timestamp(rawData_fireIncidents.arrival_dttm, "yyyy-MM-dd'T'HH:mm:ss").alias('arrival_dttm'),
        F.to_timestamp(rawData_fireIncidents.close_dttm, "yyyy-MM-dd'T'HH:mm:ss").alias('close_dttm'),
        rawData_fireIncidents.city,
        rawData_fireIncidents.zipcode,
        rawData_fireIncidents.battalion,
        rawData_fireIncidents.station_area,
        rawData_fireIncidents.suppression_units.cast('int'),
        rawData_fireIncidents.suppression_personnel.cast('int'),
        rawData_fireIncidents.ems_units.cast('int'),
        rawData_fireIncidents.ems_personnel.cast('int'),
        rawData_fireIncidents.other_units.cast('int'),
        rawData_fireIncidents.other_personnel.cast('int'),
        rawData_fireIncidents.first_unit_on_scene,
        rawData_fireIncidents.fire_fatalities.cast('int'),
        rawData_fireIncidents.fire_injuries.cast('int'),
        rawData_fireIncidents.civilian_fatalities.cast('int'),
        rawData_fireIncidents.civilian_injuries.cast('int'),
        rawData_fireIncidents.number_of_alarms.cast('int'),
        rawData_fireIncidents.primary_situation,
        rawData_fireIncidents.mutual_aid,
        rawData_fireIncidents.action_taken_primary,
        rawData_fireIncidents.action_taken_secondary,
        rawData_fireIncidents.action_taken_other,
        rawData_fireIncidents.detector_alerted_occupants,
        rawData_fireIncidents.property_use,
        rawData_fireIncidents.supervisor_district,
        rawData_fireIncidents.neighborhood_district,
        # rawData_fireIncidents.point,
        rawData_fireIncidents.point.coordinates[0].alias('point_longitude'), # longitude between -180, 180
        rawData_fireIncidents.point.coordinates[1].alias('point_latitude'), # latitude between -90, 90        
        rawData_fireIncidents.estimated_contents_loss.cast('int'),
        rawData_fireIncidents.area_of_fire_origin,
        rawData_fireIncidents.ignition_cause,
        rawData_fireIncidents.ignition_factor_primary,
        rawData_fireIncidents.ignition_factor_secondary,
        rawData_fireIncidents.heat_source,
        rawData_fireIncidents.item_first_ignited,
        rawData_fireIncidents.human_factors_associated_with_ignition,
        rawData_fireIncidents.estimated_property_loss.cast('int'),
        rawData_fireIncidents.structure_type,
        rawData_fireIncidents.structure_status,
        rawData_fireIncidents.floor_of_fire_origin.cast('int'),
        rawData_fireIncidents.fire_spread,
        rawData_fireIncidents.no_flame_spead,
        rawData_fireIncidents.number_of_floors_with_minimum_damage.cast('int'),
        rawData_fireIncidents.number_of_floors_with_significant_damage.cast('int'),
        rawData_fireIncidents.number_of_floors_with_heavy_damage.cast('int'),
        rawData_fireIncidents.number_of_floors_with_extreme_damage.cast('int'),
        rawData_fireIncidents.detectors_present,
        rawData_fireIncidents.detector_type,
        rawData_fireIncidents.detector_operation,
        rawData_fireIncidents.detector_effectiveness,
        rawData_fireIncidents.detector_failure_reason,
        rawData_fireIncidents.automatic_extinguishing_system_present,
        rawData_fireIncidents.automatic_extinguishing_sytem_type,
        rawData_fireIncidents.automatic_extinguishing_sytem_perfomance,
        rawData_fireIncidents.automatic_extinguishing_sytem_failure_reason,
        rawData_fireIncidents.number_of_sprinkler_heads_operating.cast('int'),
        rawData_fireIncidents.box
    )
)


# Save dataframe on datalake 
consumerData_fireIncidents.coalesce(1).write.mode('overwrite').parquet("/home/jovyan/datalake/consumer-data/fire_incidents")