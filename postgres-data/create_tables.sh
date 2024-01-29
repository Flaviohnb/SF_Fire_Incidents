#!/bin/bash

# Full python files paths
fire_incidents="/volume/createTable_fire_incidents.sql"
dim_calendar="/volume/createTable_dim_calendar.sql"

# PostgreSQL Env
user="postgres"
password="Postgres123!"
database="postgres"
host="127.0.0.1"
port="5432"

# Run Python scripts in sequence
echo "-------------------- running api_rawdata ---------------------"
PGPASSWORD=$password psql -U $user -h $host -p $port -d $database -a -f $fire_incidents

echo "-------------------- running CREATE TABLE dim_calendar --------------------"
PGPASSWORD=$password psql -U $user -h $host -p $port -d $database -a -f $dim_calendar

echo "-------------------- FINISHED --------------------"
