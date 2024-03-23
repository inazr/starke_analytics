#!/bin/bash

source /home/ubuntu/starke_analytics/venv/bin/activate
cd /home/ubuntu/starke_analytics/sa_dagster
dagster dev -h 0.0.0.0 -p 3000
