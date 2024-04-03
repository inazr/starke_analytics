#!/bin/bash
# This file should be executable.
# Link: https://ionic.zendesk.com/hc/en-us/articles/360000160067-Adding-execute-permissions-using-git
source /home/ubuntu/starke_analytics/venv/bin/activate
cd /home/ubuntu/starke_analytics/sa_dagster
dagster dev -h 0.0.0.0 -p 3000
