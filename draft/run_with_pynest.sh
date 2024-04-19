#!/usr/bin/env bash


export PYTHONPATH=/sensorsdata/main/program/infinity/pynest:${SKV_ADMIN_HOME}:${PYTHONPATH}
python3 /home/sa_cluster/hj/batch_insert_impala.py