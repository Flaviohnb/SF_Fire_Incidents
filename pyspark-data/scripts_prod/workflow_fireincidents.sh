#!/bin/bash

# Full python files paths
api_rawdata="/home/jovyan/scripts_prod/fireincidents_api_rawdata_overwrite.py"
rawdata_consumerdata="/home/jovyan/scripts_prod/fireincidents_rawdata_consumerdata.py"
consumerdata_datawarehouse="/home/jovyan/scripts_prod/fireincidents_consumerdata_datawarehouse.py"

# Run Python scripts in sequence
echo "-------------------- running api_rawdata ---------------------"
python3 $api_rawdata

echo "-------------------- running rawdata_consumerdata --------------------"
python3 $rawdata_consumerdata

echo "-------------------- running consumerdata_datawarehouse --------------------"
python3 $consumerdata_datawarehouse

echo "-------------------- FINISHED --------------------"
