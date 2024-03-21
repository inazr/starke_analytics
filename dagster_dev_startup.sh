#!/bin/bash

source $DAGSTER_HOME/../venv/bin/activate
cd $DAGSTER_HOME
dagster dev -h 0.0.0.0 -p 3000
