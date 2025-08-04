#!/bin/bash

# Run each script in background
/root/oracle_data_pumper/venv/bin/python /root/oracle_data_pumper/pumper_startup.py --clusterip 10.14.7.1 &
/root/oracle_data_pumper/venv/bin/python /root/oracle_data_pumper/pumper_startup.py --clusterip 10.2.197.147 &
/root/oracle_data_pumper/venv/bin/python /root/oracle_data_pumper/pumper_startup.py --clusterip 10.131.32.2 &

wait  # wait for all background jobs to finish (optional)
