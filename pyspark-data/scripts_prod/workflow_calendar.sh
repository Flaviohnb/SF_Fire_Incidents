#!/bin/bash

# Full python files paths
calendar_datawarehouse="/home/jovyan/scripts_prod/calendar_datawarehouse.py"

# Run Python scripts in sequence
echo "-------------------- running api_rawdata ---------------------"
python3 $calendar_datawarehouse

echo "-------------------- FINISHED --------------------"
